
use std::ptr;
use std::path::Path;
use std::ffi::CString;
use libmseed_sys::MS3Record;
use libmseed_sys::MS3FileParam;
use libmseed_sys::MS3TraceList;

const MS_NOERROR : i32 = libmseed_sys::MS_NOERROR as i32;
const MS_ENDOFFILE : i32 = libmseed_sys::MS_ENDOFFILE as i32;


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
pub struct MSTraceList {
    mstl: *mut MS3TraceList,
    path: String,
}

impl MSTraceList {
    pub fn new<S: AsRef<Path>>(file: S) -> Self {
        let path : String = file.as_ref().to_string_lossy().into_owned();
        let mstl : *mut MS3TraceList = ptr::null_mut();
        MSTraceList {
            mstl,
            path,
        }
    }
    pub fn read(&mut self) {
        let mspath = CString::new(self.path.clone()).unwrap();
        let verbose = 0;
        let splitversion = 0;
        let flags = libmseed_sys::MSF_UNPACKDATA;
        let tolerance = ptr::null_mut();
        let rv = unsafe {
            libmseed_sys::ms3_readtracelist((& mut self.mstl) as *mut *mut MS3TraceList,
                                            mspath.as_ptr(),
                                            tolerance,
                                            splitversion,
                                            flags,
                                            verbose)
        };
    }
    fn ptr(&self) -> MS3TraceList {
        unsafe { *self.mstl }
    }
    pub fn numtraces(&self) -> u32 {
        self.ptr().numtraces
    }
    pub fn traces_first(&self) -> &[i32] {
        use libmseed_sys::{MS3TraceSeg, MS3TraceID};
        let traces : *mut MS3TraceID = self.ptr().traces;
        let mut first : *mut MS3TraceSeg = unsafe{ (*traces).first };
        let seg : MS3TraceSeg = unsafe { *first };
        // let mut out = [];
        //if first != ptr::null_mut() {

        if seg.datasize == 0 || seg.numsamples == 0 {
            if seg.samplecnt > 0 {
                eprintln!("Data exists, but appears not be have been unpacked");
            }
            &[]
        } else {
            unsafe {
                let datasamples = (*first).datasamples;
                let out = std::slice::from_raw_parts_mut(datasamples as *mut i32, (*first).samplecnt as usize);
                out
            }
        }
        //first = unsafe{ (*first).next };
        //}

    }
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
        let sid = CString::new(self.sid()).unwrap().into_raw();
        let net = CString::new("        ").unwrap().into_raw();
        let sta = CString::new("        ").unwrap().into_raw();
        let loc = CString::new("        ").unwrap().into_raw();
        let cha = CString::new("        ").unwrap().into_raw();
        unsafe {
            libmseed_sys::ms_sid2nslc(sid, net, sta, loc, cha);
            format!("{}_{}_{}_{}",
                    CString::from_raw(net).to_string_lossy(),
                    CString::from_raw(sta).to_string_lossy(),
                    CString::from_raw(loc).to_string_lossy(),
                    CString::from_raw(cha).to_string_lossy())

        }
    }
    pub fn time(&self) -> time::OffsetDateTime {
        let mut year = 0;
        let mut yday = 0;
        let mut hour = 0;
        let mut min = 0;
        let mut sec = 0;
        let mut nsec = 0;
        unsafe {
            libmseed_sys::ms_nstime2time(self.ptr().starttime,
                                         &mut year, &mut yday, &mut hour,
                                         &mut min, &mut sec, &mut nsec);
        }
        let date = time::Date::try_from_yo(year.into(), yday ).unwrap();
        let time = time::Time::try_from_hms_nano(hour, min, sec, nsec).unwrap();
        let t = time::PrimitiveDateTime::new( date, time );
        t.assume_utc()
    }
    pub fn time_string(&self) -> String {
        let show_subseconds = 1;
        let time_format = libmseed_sys::ms_timeformat_t_SEEDORDINAL;
        let time = CString::new("                                 ").unwrap().into_raw();
        let m = self.ptr();
        unsafe {
            libmseed_sys::ms_nstime2timestr(m.starttime, time, time_format, show_subseconds)
        };
        let out = unsafe{ CString::from_raw(time).into_string().unwrap() };
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
            self.time_string(), self.time()
        )
    }
}

#[derive(Debug)]
pub enum MSError {
    EOF,
    Error(String),
}

impl MSFileParam {
    pub fn new<S: AsRef<Path>>(file: S) -> MSFileParam {
        let path : String = file.as_ref().to_string_lossy().into_owned();
        let mspath = CString::new(path.clone()).unwrap();
        let msfp : *mut MS3FileParam = ptr::null_mut();
        Self {
            path, msfp, mspath,
            fpos: 0, last: 0,
            flags: libmseed_sys::MSF_UNPACKDATA,
            verbose: 0,
        }
    }
    pub fn unpack_data(&mut self, unpack: bool) {
        if unpack {
            self.flags |= libmseed_sys::MSF_UNPACKDATA;
        } else {
            self.flags &= ! libmseed_sys::MSF_UNPACKDATA;
        }
    }
    pub fn validate_crc(&mut self, validate: bool) {
        if validate {
            self.flags |= libmseed_sys::MSF_VALIDATECRC;
        } else {
            self.flags &= ! libmseed_sys::MSF_VALIDATECRC;
        }
    }
    pub fn verbose(&mut self, verbose: bool) {
        self.verbose = if verbose { 1 } else { 0 };
    }
    pub fn filename(&self) -> &str {
        &self.path
    }
    pub fn read_record(&mut self) -> Result<MSRecord, MSError> {
        let mut msr : *mut MS3Record = ptr::null_mut();
        let rv = unsafe {
            libmseed_sys::ms3_readmsr_r((&mut self.msfp) as *mut *mut MS3FileParam,
                          (&mut msr) as *mut *mut MS3Record,
                          self.mspath.as_ptr(),
                          &mut self.fpos,
                          &mut self.last,
                          self.flags,
                          self.verbose)
        };
        if rv == MS_NOERROR {
            Ok(MSRecord( msr ))
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
            Ok( x ) => Some(Ok(x)),
            Err( MSError::EOF ) => None,
            Err( e ) => Some(Err(e)),
        }
    }
}

impl Drop for MSFileParam {
    fn drop(&mut self) {
        let mut msr : *mut MS3Record = ptr::null_mut();
        let rv = unsafe {
            libmseed_sys::ms3_readmsr_r((&mut self.msfp) as *mut *mut MS3FileParam,
                                        (&mut msr) as *mut *mut MS3Record,
                                        ptr::null_mut(),
                                        ptr::null_mut(),
                                        ptr::null_mut(),
                                        0,0)
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
        let v = fp.traces_first();
        assert_eq!(v.len(), 288000);
        assert_eq!(v[0], -47237);
        assert_eq!(v[1], -47304);
    }
    
}
