use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use io::BufWriter;
use io::Write;

use arrow::ipc::writer::StreamWriter;

use arrow::array::Float32Array;
use arrow::array::Int64Array;
use arrow::array::StringArray;

use arrow::record_batch::RecordBatch;

use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;

use sysinfo::Pid;
use sysinfo::Process;

pub struct IpcStreamWriter<W: Write>(pub StreamWriter<BufWriter<W>>);

impl<W: Write> IpcStreamWriter<W> {
    pub fn write(&mut self, b: &RecordBatch) -> Result<(), io::Error> {
        self.0.write(b).map_err(io::Error::other)
    }

    pub fn flush(&mut self) -> Result<(), io::Error> {
        self.0.flush().map_err(io::Error::other)
    }
    pub fn finish(&mut self) -> Result<(), io::Error> {
        self.0.finish().map_err(io::Error::other)
    }
}

pub struct SystemInfo(pub sysinfo::System);

impl Default for SystemInfo {
    fn default() -> Self {
        Self::new()
    }
}

use arrow::compute::concat_batches;

pub struct ProcsBatchIterator<'a> {
    procs_iter: std::collections::hash_map::Values<'a, Pid, Process>,
    schema: SchemaRef,
    max_rows_per_batch: usize,
}

impl<'a> Iterator for ProcsBatchIterator<'a> {
    type Item = Result<RecordBatch, io::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut batches = Vec::new();
        for proc in self.procs_iter.by_ref().take(self.max_rows_per_batch) {
            let proc_info = ProcessInfo(proc);
            match proc_info.to_batch(self.schema.clone()) {
                Ok(batch) => batches.push(batch),
                Err(e) => return Some(Err(e)),
            }
        }

        if batches.is_empty() {
            return None;
        }

        match concat_batches(&self.schema, &batches) {
            Ok(batch) => Some(Ok(batch)),
            Err(e) => Some(Err(io::Error::other(e))),
        }
    }
}

pub const ROWS_PER_BATCH_DEFAULT: usize = 1024;

impl SystemInfo {
    pub fn new() -> Self {
        Self(sysinfo::System::new_all())
    }

    pub fn refresh(&mut self) {
        self.0.refresh_all()
    }

    pub fn get_proc_map(&self) -> &HashMap<Pid, Process> {
        self.0.processes()
    }

    pub fn procs2batch<'a>(
        &'a self,
        max_rows_per_batch: usize,
    ) -> Result<ProcsBatchIterator<'a>, io::Error> {
        let schema = Arc::new(ProcessInfo::schema());
        let procs_iter = self.get_proc_map().values();

        Ok(ProcsBatchIterator {
            procs_iter,
            schema,
            max_rows_per_batch,
        })
    }

    pub fn into_writer<W: Write>(
        self,
        mut wtr: IpcStreamWriter<W>,
        rows_per_batch: usize,
    ) -> Result<(), io::Error> {
        let batches = self.procs2batch(rows_per_batch)?;
        for batch in batches {
            wtr.write(&batch?)?;
        }
        wtr.flush()?;
        wtr.finish()
    }

    pub fn procs2ipc2stdout(rows_per_batch: usize) -> Result<(), io::Error> {
        let mut o = io::stdout().lock();
        let schema: SchemaRef = ProcessInfo::schema().into();
        let swtr: StreamWriter<_> =
            StreamWriter::try_new_buffered(&mut o, &schema).map_err(io::Error::other)?;
        let iwtr = IpcStreamWriter(swtr);
        let mut si = SystemInfo::new();
        si.refresh();
        si.into_writer(iwtr, rows_per_batch)?;
        o.flush()
    }
}

pub struct ProcessInfo<'a>(pub &'a Process);

impl<'a> ProcessInfo<'a> {
    pub fn schema() -> Schema {
        Schema::new(vec![
            Field::new("pid", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("cpu_usage", DataType::Float32, false),
            Field::new("memory", DataType::Int64, false),
            Field::new("virtual_memory", DataType::Int64, false),
            Field::new("start_time", DataType::Int64, false),
            Field::new("run_time", DataType::Int64, false),
            Field::new("status", DataType::Utf8, true),
            Field::new("parent", DataType::Int64, true),
            Field::new("user_id", DataType::Int64, true),
            Field::new("group_id", DataType::Int64, true),
            Field::new("effective_user_id", DataType::Int64, true),
            Field::new("effective_group_id", DataType::Int64, true),
            Field::new("session_id", DataType::Int64, true),
            Field::new("accumulated_cpu_time", DataType::Int64, false),
        ])
    }

    /// Converts the process info to a batch(with single row).
    pub fn to_batch(&self, schema: SchemaRef) -> Result<RecordBatch, io::Error> {
        let pid_array = Int64Array::from(vec![self.pid().as_u32() as i64]);
        let name_array = StringArray::from(vec![self.name()]);
        let cpu_usage_array = Float32Array::from(vec![self.cpu_usage()]);
        let memory_array = Int64Array::from(vec![self.memory() as i64]);
        let virtual_memory_array = Int64Array::from(vec![self.virtual_memory() as i64]);
        let start_time_array = Int64Array::from(vec![self.start_time() as i64]);
        let run_time_array = Int64Array::from(vec![self.run_time() as i64]);
        let status_array = StringArray::from(vec![self.status()]);
        let parent_array = Int64Array::from(vec![self.parent()]);
        let user_id_array = Int64Array::from(vec![self.user_id()]);
        let group_id_array = Int64Array::from(vec![self.group_id()]);
        let effective_user_id_array = Int64Array::from(vec![self.effective_user_id()]);
        let effective_group_id_array = Int64Array::from(vec![self.effective_group_id()]);
        let session_id_array = Int64Array::from(vec![self.session_id()]);
        let accumulated_cpu_time_array = Int64Array::from(vec![self.accumulated_cpu_time() as i64]);

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(pid_array),
                Arc::new(name_array),
                Arc::new(cpu_usage_array),
                Arc::new(memory_array),
                Arc::new(virtual_memory_array),
                Arc::new(start_time_array),
                Arc::new(run_time_array),
                Arc::new(status_array),
                Arc::new(parent_array),
                Arc::new(user_id_array),
                Arc::new(group_id_array),
                Arc::new(effective_user_id_array),
                Arc::new(effective_group_id_array),
                Arc::new(session_id_array),
                Arc::new(accumulated_cpu_time_array),
            ],
        )
        .map_err(io::Error::other)
    }

    pub fn pid(&self) -> Pid {
        self.0.pid()
    }

    pub fn name(&self) -> String {
        self.0.name().to_string_lossy().into_owned()
    }

    pub fn cpu_usage(&self) -> f32 {
        self.0.cpu_usage()
    }

    pub fn memory(&self) -> u64 {
        self.0.memory()
    }

    pub fn virtual_memory(&self) -> u64 {
        self.0.virtual_memory()
    }

    pub fn start_time(&self) -> u64 {
        self.0.start_time()
    }

    pub fn run_time(&self) -> u64 {
        self.0.run_time()
    }

    pub fn status(&self) -> String {
        self.0.status().to_string()
    }

    pub fn parent(&self) -> Option<i64> {
        self.0.parent().map(|p| p.as_u32() as i64)
    }

    pub fn user_id(&self) -> Option<i64> {
        self.0.user_id().map(|u| **u as i64)
    }

    pub fn group_id(&self) -> Option<i64> {
        self.0.group_id().map(|g| *g as i64)
    }

    pub fn effective_user_id(&self) -> Option<i64> {
        self.0.effective_user_id().map(|u| **u as i64)
    }

    pub fn effective_group_id(&self) -> Option<i64> {
        self.0.effective_group_id().map(|g| *g as i64)
    }

    pub fn session_id(&self) -> Option<i64> {
        self.0.session_id().map(|p| p.as_u32() as i64)
    }

    pub fn accumulated_cpu_time(&self) -> u64 {
        self.0.accumulated_cpu_time()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema() {
        let schema = ProcessInfo::schema();
        let fields = schema.fields();

        assert_eq!(fields.len(), 15);

        assert_eq!(fields[0].name(), "pid");
        assert_eq!(fields[0].data_type(), &DataType::Int64);
        assert!(!fields[0].is_nullable());

        assert_eq!(fields[1].name(), "name");
        assert_eq!(fields[1].data_type(), &DataType::Utf8);
        assert!(!fields[1].is_nullable());

        assert_eq!(fields[2].name(), "cpu_usage");
        assert_eq!(fields[2].data_type(), &DataType::Float32);
        assert!(!fields[2].is_nullable());

        assert_eq!(fields[3].name(), "memory");
        assert_eq!(fields[3].data_type(), &DataType::Int64);
        assert!(!fields[3].is_nullable());

        assert_eq!(fields[4].name(), "virtual_memory");
        assert_eq!(fields[4].data_type(), &DataType::Int64);
        assert!(!fields[4].is_nullable());

        assert_eq!(fields[5].name(), "start_time");
        assert_eq!(fields[5].data_type(), &DataType::Int64);
        assert!(!fields[5].is_nullable());

        assert_eq!(fields[6].name(), "run_time");
        assert_eq!(fields[6].data_type(), &DataType::Int64);
        assert!(!fields[6].is_nullable());

        assert_eq!(fields[7].name(), "status");
        assert_eq!(fields[7].data_type(), &DataType::Utf8);
        assert!(fields[7].is_nullable());

        assert_eq!(fields[8].name(), "parent");
        assert_eq!(fields[8].data_type(), &DataType::Int64);
        assert!(fields[8].is_nullable());

        assert_eq!(fields[9].name(), "user_id");
        assert_eq!(fields[9].data_type(), &DataType::Int64);
        assert!(fields[9].is_nullable());

        assert_eq!(fields[10].name(), "group_id");
        assert_eq!(fields[10].data_type(), &DataType::Int64);
        assert!(fields[10].is_nullable());

        assert_eq!(fields[11].name(), "effective_user_id");
        assert_eq!(fields[11].data_type(), &DataType::Int64);
        assert!(fields[11].is_nullable());

        assert_eq!(fields[12].name(), "effective_group_id");
        assert_eq!(fields[12].data_type(), &DataType::Int64);
        assert!(fields[12].is_nullable());

        assert_eq!(fields[13].name(), "session_id");
        assert_eq!(fields[13].data_type(), &DataType::Int64);
        assert!(fields[13].is_nullable());

        assert_eq!(fields[14].name(), "accumulated_cpu_time");
        assert_eq!(fields[14].data_type(), &DataType::Int64);
        assert!(!fields[14].is_nullable());
    }
}
