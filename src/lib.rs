use libmseed_sys::MS3FileParam;
use libmseed_sys::MS3Record;
use libmseed_sys::MS3TraceID;
use libmseed_sys::MS3TraceList;
use libmseed_sys::MS3TraceSeg;
use std::ffi::CString;
use std::path::Path;
use std::ptr;

use std::slice::from_raw_parts;

const MS_NOERROR: i32 = libmseed_sys::MS_NOERROR as i32;
const MS_ENDOFFILE: i32 = libmseed_sys::MS_ENDOFFILE as i32;

#[derive(Debug)]
pub struct MSRecord(*mut MS3Record);

#[derive(Debug)]
pub struct MSFileParam {
    path: String,
    mspath: CString,
    msfp: *mut MS3FileParam,
    fpos: i64,
    last: i8,
    verbose: i8,
    flags: u32,
}

#[derive(Debug)]
pub struct MSTraceList {
    // Pointer to Miniseed Trace List
    mstl: *mut MS3TraceList,
    // Miniseed file name
    path: String,
}

#[derive(Debug)]
pub struct MSTraceID(*mut MS3TraceID);
#[derive(Debug)]
pub struct MSTraceSegment(*mut MS3TraceSeg);

#[derive(Debug)]
pub struct MSTraceIDIterator {
    mstid: *mut MS3TraceID,
}
#[derive(Debug)]
pub struct MSTraceSegmentIterator {
    mstseg: *mut MS3TraceSeg,
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum MSSampleType {
    Integer32,
    Float32,
    Float64,
}

#[derive(Debug)]
pub enum MSError {
    EOF,
    Error(String),
}

impl MSTraceList {
    pub fn new<S: AsRef<Path>>(file: S) -> Self {
        let path: String = file.as_ref().to_string_lossy().into_owned();
        let mstl: *mut MS3TraceList = ptr::null_mut();
        MSTraceList { mstl, path }
    }
    pub fn read(&mut self) {
        let mspath = CString::new(self.path.clone()).unwrap();
        let verbose = 0;
        let splitversion = 0;
        let flags = libmseed_sys::MSF_UNPACKDATA;
        let tolerance = ptr::null_mut();
        let rv = unsafe {
            libmseed_sys::ms3_readtracelist(
                (&mut self.mstl) as *mut *mut MS3TraceList,
                mspath.as_ptr(),
                tolerance,
                splitversion,
                flags,
                verbose,
            )
        };
        assert_eq!(rv, MS_NOERROR);
    }
    fn ptr(&self) -> MS3TraceList {
        unsafe { *self.mstl }
    }
    pub fn numtraces(&self) -> u32 {
        self.ptr().numtraces
    }
    pub fn traces(&self) -> MSTraceIDIterator {
        MSTraceIDIterator {
            mstid: self.ptr().traces,
        }
    }
}

impl MSTraceID {
    fn ptr(&self) -> MS3TraceID {
        unsafe { *self.0 }
    }
    pub fn segments(&self) -> MSTraceSegmentIterator {
        MSTraceSegmentIterator {
            mstseg: self.ptr().first,
        }
    }
    pub fn network(&self) -> String {
        sid_to_nslc(&self.ptr().sid).net
    }
    pub fn station(&self) -> String {
        sid_to_nslc(&self.ptr().sid).sta
    }
    pub fn location(&self) -> String {
        sid_to_nslc(&self.ptr().sid).loc
    }
    pub fn channel(&self) -> String {
        sid_to_nslc(&self.ptr().sid).cha
    }
    pub fn start_time(&self) -> time::OffsetDateTime {
        nstime_to_time(self.ptr().earliest)
    }
    pub fn end_time(&self) -> time::OffsetDateTime {
        nstime_to_time(self.ptr().latest)
    }
    pub fn pubversion(&self) -> u8 {
        self.ptr().pubversion
    }
    pub fn numsegments(&self) -> u32 {
        self.ptr().numsegments
    }
}

impl Iterator for MSTraceIDIterator {
    type Item = MSTraceID;
    fn next(&mut self) -> Option<Self::Item> {
        if (*self).mstid == ptr::null_mut() {
            None
        } else {
            let prev = self.mstid;
            self.mstid = unsafe { (*self.mstid).next };
            Some(MSTraceID(prev))
        }
    }
}

impl Iterator for MSTraceSegmentIterator {
    type Item = MSTraceSegment;
    fn next(&mut self) -> Option<Self::Item> {
        if self.mstseg == ptr::null_mut() {
            None
        } else {
            let prev = self.mstseg;
            self.mstseg = unsafe { (*self.mstseg).next };
            Some(MSTraceSegment(prev))
        }
    }
}

impl MSSampleType {
    pub fn as_char(&self) -> i8 {
        match self {
            MSSampleType::Integer32 => 'i' as i8,
            MSSampleType::Float32 => 'f' as i8,
            MSSampleType::Float64 => 'd' as i8,
        }
    }
}

impl MSTraceSegment {
    fn ptr(&self) -> MS3TraceSeg {
        unsafe { *self.0 }
    }
    fn sampletype(&self) -> MSSampleType {
        let s = self.ptr();
        match s.sampletype {
            105 => MSSampleType::Integer32, // i
            102 => MSSampleType::Float32,   // f
            100 => MSSampleType::Float64,   // d
            _ => panic!("Unknown sample type: {}", s.sampletype),
        }
    }
    pub fn start_time(&self) -> time::OffsetDateTime {
        nstime_to_time(self.ptr().starttime)
    }
    pub fn end_time(&self) -> time::OffsetDateTime {
        nstime_to_time(self.ptr().endtime)
    }
    pub fn samprate(&self) -> f64 {
        self.ptr().samprate
    }
    pub fn samplecnt(&self) -> i64 {
        self.ptr().samplecnt
    }
    pub fn numsamples(&self) -> i64 {
        self.ptr().numsamples
    }
    pub fn datasize(&self) -> u64 {
        self.ptr().datasize
    }
    pub fn data_unpacked(&self) -> bool {
        self.samplecnt() == self.numsamples() && self.datasize() > 0
    }

    fn convert_data(&self, t: MSSampleType) -> bool {
        if !self.data_unpacked() {
            return false;
        }
        let truncate = 0;
        if t != self.sampletype() {
            unsafe { libmseed_sys::mstl3_convertsamples(self.0, t.as_char(), truncate) == 0 }
        } else {
            true
        }
    }
    pub fn to_vec_i32(&self) -> Vec<i32> {
        if !self.data_unpacked() {
            return vec![];
        }
        self.convert_data(MSSampleType::Integer32);
        let s = self.ptr();
        unsafe { from_raw_parts(s.datasamples as *mut i32, s.samplecnt as usize) }.to_vec()
    }
    pub fn to_vec_f32(&self) -> Vec<f32> {
        if !self.data_unpacked() {
            return vec![];
        }
        self.convert_data(MSSampleType::Float32);
        let s = self.ptr();
        unsafe { from_raw_parts(s.datasamples as *mut f32, s.samplecnt as usize) }.to_vec()
    }
    pub fn to_vec_f64(&self) -> Vec<f64> {
        if !self.data_unpacked() {
            return vec![];
        }
        self.convert_data(MSSampleType::Float64);
        let s = self.ptr();
        unsafe { from_raw_parts(s.datasamples as *mut f64, s.samplecnt as usize) }.to_vec()
    }
}
struct NSLC {
    net: String,
    sta: String,
    loc: String,
    cha: String,
}

fn sid_to_nslc(sid: &[i8]) -> NSLC {
    let s0 = "               ";
    let sid = CString::new(i8_to_string(sid)).unwrap().into_raw();
    let xnet = CString::new(s0).unwrap().into_raw();
    let xsta = CString::new(s0).unwrap().into_raw();
    let xloc = CString::new(s0).unwrap().into_raw();
    let xcha = CString::new(s0).unwrap().into_raw();
    unsafe {
        libmseed_sys::ms_sid2nslc(sid, xnet, xsta, xloc, xcha);
        let net = CString::from_raw(xnet).into_string().unwrap();
        let sta = CString::from_raw(xsta).into_string().unwrap();
        let loc = CString::from_raw(xloc).into_string().unwrap();
        let cha = CString::from_raw(xcha).into_string().unwrap();
        NSLC { net, sta, loc, cha }
    }
}

fn nstime_to_time(nst: i64) -> time::OffsetDateTime {
    let mut year = 0;
    let mut yday = 0;
    let mut hour = 0;
    let mut min = 0;
    let mut sec = 0;
    let mut nsec = 0;
    unsafe {
        libmseed_sys::ms_nstime2time(
            nst, &mut year, &mut yday, &mut hour, &mut min, &mut sec, &mut nsec,
        );
    }
    let date = time::Date::try_from_yo(year.into(), yday).unwrap();
    let time = time::Time::try_from_hms_nano(hour, min, sec, nsec).unwrap();
    let t = time::PrimitiveDateTime::new(date, time);
    t.assume_utc()
}

impl MSRecord {
    fn ptr(&self) -> MS3Record {
        unsafe { *self.0 }
    }
    pub fn numsamples(&self) -> i64 {
        self.ptr().numsamples
    }
    pub fn sid(&self) -> String {
        i8_to_string(&(self.ptr().sid))
    }
    pub fn id(&self) -> String {
        let v = sid_to_nslc(&self.ptr().sid);
        format!("{}_{}_{}_{}", v.net, v.sta, v.loc, v.cha)
    }
    pub fn network(&self) -> String {
        sid_to_nslc(&self.ptr().sid).net
    }
    pub fn station(&self) -> String {
        sid_to_nslc(&self.ptr().sid).sta
    }
    pub fn location(&self) -> String {
        sid_to_nslc(&self.ptr().sid).loc
    }
    pub fn channel(&self) -> String {
        sid_to_nslc(&self.ptr().sid).cha
    }
    pub fn start_time(&self) -> time::OffsetDateTime {
        nstime_to_time(self.ptr().starttime)
    }
    pub fn time_string(&self) -> String {
        let show_subseconds = 1;
        let time_format = libmseed_sys::ms_timeformat_t_SEEDORDINAL;
        let time = CString::new("                                 ")
            .unwrap()
            .into_raw();
        let m = self.ptr();
        unsafe { libmseed_sys::ms_nstime2timestr(m.starttime, time, time_format, show_subseconds) };
        let out = unsafe { CString::from_raw(time).into_string().unwrap() };
        out
    }
}
fn i8_to_string(vin: &[i8]) -> String {
    let v: Vec<u8> = vin
        .iter()
        .map(|x| *x as u8) // cast i8 as u8
        .filter(|x| *x != 0u8) // remove null terminators
        .collect();
    String::from_utf8(v).unwrap() // convert to  string
}

use std::fmt;
impl fmt::Display for MSRecord {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let v = self.ptr();
        write!(
            f,
            "{}, {}, {}, {} samples, {} Hz, {} {}",
            self.sid(),
            v.pubversion,
            v.reclen,
            v.samplecnt,
            v.samprate,
            self.time_string(),
            self.start_time()
        )
    }
}

impl MSFileParam {
    pub fn new<S: AsRef<Path>>(file: S) -> MSFileParam {
        let path: String = file.as_ref().to_string_lossy().into_owned();
        let mspath = CString::new(path.clone()).unwrap();
        let msfp: *mut MS3FileParam = ptr::null_mut();
        Self {
            path,
            msfp,
            mspath,
            fpos: 0,
            last: 0,
            flags: libmseed_sys::MSF_UNPACKDATA,
            verbose: 0,
        }
    }
    pub fn unpack_data(&mut self, unpack: bool) {
        if unpack {
            self.flags |= libmseed_sys::MSF_UNPACKDATA;
        } else {
            self.flags &= !libmseed_sys::MSF_UNPACKDATA;
        }
    }
    pub fn validate_crc(&mut self, validate: bool) {
        if validate {
            self.flags |= libmseed_sys::MSF_VALIDATECRC;
        } else {
            self.flags &= !libmseed_sys::MSF_VALIDATECRC;
        }
    }
    pub fn verbose(&mut self, verbose: bool) {
        self.verbose = if verbose { 1 } else { 0 };
    }
    pub fn filename(&self) -> &str {
        &self.path
    }
    pub fn read_record(&mut self) -> Result<MSRecord, MSError> {
        let mut msr: *mut MS3Record = ptr::null_mut();
        let rv = unsafe {
            libmseed_sys::ms3_readmsr_r(
                (&mut self.msfp) as *mut *mut MS3FileParam,
                (&mut msr) as *mut *mut MS3Record,
                self.mspath.as_ptr(),
                &mut self.fpos,
                &mut self.last,
                self.flags,
                self.verbose,
            )
        };
        if rv == MS_NOERROR {
            Ok(MSRecord(msr))
        } else if rv == MS_ENDOFFILE {
            Err(MSError::EOF)
        } else {
            Err(MSError::Error(format!("Error: {}", rv)))
        }
    }
}

impl Iterator for MSFileParam {
    type Item = Result<MSRecord, MSError>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.read_record() {
            Ok(x) => Some(Ok(x)),
            Err(MSError::EOF) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

impl Drop for MSFileParam {
    fn drop(&mut self) {
        let mut msr: *mut MS3Record = ptr::null_mut();
        let rv = unsafe {
            libmseed_sys::ms3_readmsr_r(
                (&mut self.msfp) as *mut *mut MS3FileParam,
                (&mut msr) as *mut *mut MS3Record,
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                0,
                0,
            )
        };
        assert!(rv == MS_NOERROR);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn file_param() {
        let fp = MSFileParam::new("./tests/multiple.seed");
        for r in fp {
            if let Ok(rec) = r {
                println!("{} {}", rec, rec.numsamples());
            }
        }
    }
    #[test]
    fn trace_list() {
        let mut fp = MSTraceList::new("./tests/multiple.seed");
        fp.read();
        assert_eq!(fp.numtraces(), 1);
        for trace in fp.traces() {
            for segment in trace.segments() {
                let out = segment.to_vec_i32();
                assert_eq!(out.len(), 288000);
            }
        }
    }
}
