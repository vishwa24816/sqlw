use std::io::{self, Read, Write};

pub trait Conn: Read + Write + Send {}
impl<T: Read + Write + Send> Conn for T {}

#[cfg(windows)]
pub fn pipe_name(endpoint: &str) -> String {
    if endpoint.starts_with(r"\\.\pipe\") {
        endpoint.to_string()
    } else {
        format!(r"\\.\pipe\{endpoint}")
    }
}

/// Named-pipe server primitives.
///
/// ponytail: `std::os::windows::named_pipe` no longer exists in Rust 1.95, so
/// this is a minimal kernel32/advapi32 shim (create/connect only; I/O goes
/// through std `File`). Upgrade path: `windows-sys` if more pipe features
/// (overlapped I/O, PeekNamedPipe) are ever needed.
#[cfg(windows)]
mod win {
    use std::fs::File;
    use std::io;
    use std::mem::size_of;
    use std::os::windows::io::{AsRawHandle, FromRawHandle, RawHandle};
    use std::ptr;

    const FILE_FLAG_FIRST_PIPE_INSTANCE: u32 = 0x0008_0000;
    const PIPE_ACCESS_DUPLEX: u32 = 0x3;
    // PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT == 0
    const PIPE_REJECT_REMOTE_CLIENTS: u32 = 0x8;
    const INVALID_HANDLE_VALUE: isize = -1;
    const ERROR_PIPE_CONNECTED: i32 = 535;
    // Client opened and closed the pipe between create and ConnectNamedPipe.
    pub(super) const ERROR_NO_DATA: i32 = 232;

    // advapi32 security-descriptor construction: a DACL granting only the
    // current user (service account). No crates; the buffer layout is stable
    // Win32 ABI. Upgrade path: `windows-sys` SECURITY_ATTRIBUTES builder.
    const TOKEN_QUERY: u32 = 0x0008;
    const TOKEN_USER: i32 = 1;
    const SECURITY_DESCRIPTOR_REVISION: u32 = 1;
    const ACL_REVISION: u32 = 4;
    const GENERIC_ALL: u32 = 0x1000_0000;

    #[repr(C)]
    struct SecurityAttributes {
        n_length: u32,
        lp_security_descriptor: *mut core::ffi::c_void,
        b_inherit_handle: i32,
    }

    #[repr(C)]
    struct SecurityDescriptor {
        revision: u8,
        sbz1: u8,
        control: u16,
        owner: *mut core::ffi::c_void,
        group: *mut core::ffi::c_void,
        sacl: *mut core::ffi::c_void,
        dacl: *mut core::ffi::c_void,
    }

    #[repr(C)]
    struct SidAndAttributes {
        sid: *mut core::ffi::c_void,
        attributes: u32,
    }

    #[repr(C)]
    struct TokenUser {
        user: SidAndAttributes,
    }

    #[link(name = "advapi32")]
    extern "system" {
        fn OpenProcessToken(
            process_handle: isize,
            desired_access: u32,
            token_handle: *mut isize,
        ) -> i32;
        fn GetTokenInformation(
            token_handle: isize,
            token_information_class: i32,
            token_information: *mut core::ffi::c_void,
            token_information_length: u32,
            return_length: *mut u32,
        ) -> i32;
        fn InitializeSecurityDescriptor(
            security_descriptor: *mut core::ffi::c_void,
            revision: u32,
        ) -> i32;
        fn SetSecurityDescriptorDacl(
            security_descriptor: *mut core::ffi::c_void,
            dacl_present: i32,
            dacl: *mut core::ffi::c_void,
            dacl_defaulted: i32,
        ) -> i32;
        fn InitializeAcl(acl: *mut core::ffi::c_void, acl_length: u32, revision: u32) -> i32;
        fn AddAccessAllowedAce(
            acl: *mut core::ffi::c_void,
            revision: u32,
            access_mask: u32,
            sid: *mut core::ffi::c_void,
        ) -> i32;
        fn GetLengthSid(sid: *mut core::ffi::c_void) -> u32;
    }

    #[link(name = "kernel32")]
    extern "system" {
        fn CreateNamedPipeW(
            lp_name: *const u16,
            open_mode: u32,
            pipe_mode: u32,
            max_instances: u32,
            out_buffer_size: u32,
            in_buffer_size: u32,
            default_timeout: u32,
            security_attributes: *mut core::ffi::c_void,
        ) -> isize;
        fn ConnectNamedPipe(handle: isize, overlapped: *mut core::ffi::c_void) -> i32;
        fn GetCurrentProcess() -> isize;
        fn CloseHandle(handle: isize) -> i32;
    }

    /// Owns the security material used for exactly one CreateNamedPipeW call.
    /// All pointed-to buffers are either heap-stable (Vec/Box) or outlive the
    /// call, so the struct may be moved freely.
    struct PipeSec {
        attr: SecurityAttributes,
        _desc: Box<SecurityDescriptor>,
        _acl: Vec<u8>,
        _token_buf: Vec<u8>,
        token: isize,
    }

    impl Drop for PipeSec {
        fn drop(&mut self) {
            unsafe { CloseHandle(self.token) };
        }
    }

    fn last_err(step: &str) -> io::Error {
        let e = io::Error::last_os_error();
        io::Error::new(e.kind(), format!("{step}: {e}"))
    }

    fn build_security_attributes() -> io::Result<PipeSec> {
        unsafe {
            let mut token: isize = 0;
            if OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) == 0 {
                return Err(last_err("OpenProcessToken"));
            }
            // First call sizes the buffer and reports ERROR_INSUFFICIENT_BUFFER.
            let mut needed: u32 = 0;
            GetTokenInformation(token, TOKEN_USER, ptr::null_mut(), 0, &mut needed);
            if needed == 0 {
                CloseHandle(token);
                return Err(last_err("GetTokenInformation(size)"));
            }
            let mut token_buf = vec![0u8; needed as usize];
            if GetTokenInformation(
                token,
                TOKEN_USER,
                token_buf.as_mut_ptr() as *mut _,
                needed,
                &mut needed,
            ) == 0
            {
                CloseHandle(token);
                return Err(last_err("GetTokenInformation"));
            }
            let user = &*(token_buf.as_ptr() as *const TokenUser);
            let sid = user.user.sid;
            let sid_len = GetLengthSid(sid);
            if sid_len == 0 {
                CloseHandle(token);
                return Err(last_err("GetLengthSid"));
            }
            // ACL header (8) + ACCESS_ALLOWED_ACE header (8) + mask (4) + SID,
            // with slack for alignment rounding.
            let acl_len = 8 + 12 + sid_len + 16;
            let mut acl = vec![0u8; acl_len as usize];
            if InitializeAcl(acl.as_mut_ptr() as *mut _, acl_len, ACL_REVISION) == 0 {
                CloseHandle(token);
                return Err(last_err("InitializeAcl"));
            }
            if AddAccessAllowedAce(acl.as_mut_ptr() as *mut _, ACL_REVISION, GENERIC_ALL, sid) == 0
            {
                CloseHandle(token);
                return Err(last_err("AddAccessAllowedAce"));
            }
            let mut desc = Box::new(SecurityDescriptor {
                revision: 0,
                sbz1: 0,
                control: 0,
                owner: ptr::null_mut(),
                group: ptr::null_mut(),
                sacl: ptr::null_mut(),
                dacl: ptr::null_mut(),
            });
            if InitializeSecurityDescriptor(
                &mut *desc as *mut _ as *mut _,
                SECURITY_DESCRIPTOR_REVISION,
            ) == 0
            {
                CloseHandle(token);
                return Err(last_err("InitializeSecurityDescriptor"));
            }
            if SetSecurityDescriptorDacl(
                &mut *desc as *mut _ as *mut _,
                1,
                acl.as_mut_ptr() as *mut _,
                0,
            ) == 0
            {
                CloseHandle(token);
                return Err(last_err("SetSecurityDescriptorDacl"));
            }
            Ok(PipeSec {
                attr: SecurityAttributes {
                    n_length: size_of::<SecurityAttributes>() as u32,
                    lp_security_descriptor: &mut *desc as *mut _ as *mut _,
                    b_inherit_handle: 0,
                },
                _desc: desc,
                _acl: acl,
                _token_buf: token_buf,
                token,
            })
        }
    }

    pub fn create_instance(name_wide: &[u16], first: bool) -> io::Result<File> {
        let open_mode = PIPE_ACCESS_DUPLEX
            | if first {
                FILE_FLAG_FIRST_PIPE_INSTANCE
            } else {
                0
            };
        let sec = build_security_attributes()?;
        unsafe {
            let h = CreateNamedPipeW(
                name_wide.as_ptr(),
                open_mode,
                // Named pipes cannot be redirected over the network.
                PIPE_REJECT_REMOTE_CLIENTS,
                255, // Windows caps named-pipe instances at 255 per name
                0,
                0,
                0,
                &sec.attr as *const _ as *mut _,
            );
            if h == INVALID_HANDLE_VALUE {
                return Err(io::Error::last_os_error());
            }
            Ok(File::from_raw_handle(h as RawHandle))
        }
    }

    pub fn wait_connected(file: &File) -> io::Result<()> {
        unsafe {
            if ConnectNamedPipe(file.as_raw_handle() as isize, ptr::null_mut()) != 0 {
                return Ok(());
            }
            let e = io::Error::last_os_error();
            if e.raw_os_error() == Some(ERROR_PIPE_CONNECTED) {
                return Ok(()); // client connected between create and connect
            }
            Err(e)
        }
    }
}

#[cfg(windows)]
pub struct Listener {
    name_wide: Vec<u16>,
    next: Option<std::fs::File>,
}

/// Bind the single-writer endpoint. `FILE_FLAG_FIRST_PIPE_INSTANCE` makes the
/// ownership claim atomic across processes: if any instance of the pipe already
/// exists, the create fails and no second writer can start.
#[cfg(windows)]
pub fn bind(endpoint: &str) -> io::Result<Listener> {
    let name = pipe_name(endpoint);
    let mut name_wide: Vec<u16> = name.encode_utf16().collect();
    name_wide.push(0);
    let first = win::create_instance(&name_wide, true).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!(
                "cannot acquire writer endpoint {name} (is another sqlw service running?): {e}"
            ),
        )
    })?;
    Ok(Listener {
        name_wide,
        next: Some(first),
    })
}

#[cfg(windows)]
impl Listener {
    pub fn accept(&mut self) -> io::Result<Box<dyn Conn>> {
        loop {
            let current = match self.next.take() {
                Some(f) => f,
                None => win::create_instance(&self.name_wide, false)?,
            };
            match win::wait_connected(&current) {
                Ok(()) => return self.finish_accept(current),
                // A client connected and closed in the race window: that
                // instance is dead, retire it and wait on a fresh one.
                Err(e) if e.raw_os_error() == Some(win::ERROR_NO_DATA) => continue,
                Err(e) => return Err(e),
            }
        }
    }

    fn finish_accept(&mut self, current: std::fs::File) -> io::Result<Box<dyn Conn>> {
        // Best effort: if the spare cannot be created (instance churn), keep
        // serving this connection; the next accept retries the spare.
        match win::create_instance(&self.name_wide, false) {
            Ok(spare) => self.next = Some(spare),
            Err(e) => eprintln!("sqlw: pipe spare instance failed (will retry): {e}"),
        }
        Ok(Box::new(current))
    }
}

#[cfg(unix)]
pub struct Listener {
    inner: std::os::unix::net::UnixListener,
    /// flock held for the process lifetime = instance lock (released on exit
    /// or crash by the kernel, so a SIGKILL cannot leak ownership).
    _lock: std::fs::File,
    #[cfg(target_os = "linux")]
    self_uid: u32,
}

// mode_t: 32-bit on Linux/Android, 16-bit on macOS/BSD.
#[cfg(unix)]
#[cfg(any(target_os = "linux", target_os = "android"))]
type RawMode = u32;
#[cfg(unix)]
#[cfg(not(any(target_os = "linux", target_os = "android")))]
type RawMode = u16;

#[cfg(unix)]
const LOCK_EX: i32 = 2;
#[cfg(unix)]
const LOCK_NB: i32 = 4;

#[cfg(unix)]
extern "C" {
    fn flock(fd: i32, operation: i32) -> i32;
    fn umask(mask: RawMode) -> RawMode;
    fn geteuid() -> u32;
}

/// Bind the single-writer endpoint.
///
/// Ownership: a nonblocking exclusive `flock` on `{endpoint}.lock`, taken
/// before the socket is touched and held for the process lifetime — a second
/// service instance fails here, before it can open the database. The stale
/// socket of a crashed process is detected under the lock and replaced only
/// when the path really is an unconnected socket.
#[cfg(unix)]
pub fn bind(endpoint: &str) -> io::Result<Listener> {
    use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};

    // Private runtime directory: create the leaf dir if missing, then tighten
    // it to 0700 unconditionally — the lock and socket live here, so a loose
    // pre-existing directory (or a lost creation race) must not weaken that.
    if let Some(parent) = std::path::Path::new(endpoint).parent() {
        if !parent.as_os_str().is_empty() {
            match std::fs::create_dir(parent) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
                Err(e) => {
                    return Err(io::Error::new(
                        e.kind(),
                        format!("cannot create socket directory {}: {e}", parent.display()),
                    ))
                }
            }
            std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700)).map_err(
                |e| {
                    io::Error::new(
                        e.kind(),
                        format!(
                            "cannot restrict socket directory {} to 0700: {e}",
                            parent.display()
                        ),
                    )
                },
            )?;
        }
    }

    // Instance lock, held for this process's lifetime. A lock file planted
    // while the runtime dir was still loose can keep blocking flock after
    // the 0700 chmod; if we don't own it, unlink it (the directory is ours
    // now) and retry once, so a foreign squat can't DoS us forever.
    let lock_path = format!("{endpoint}.lock");
    let mut retry_foreign = true;
    let lock = loop {
        let f = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&lock_path)
            .map_err(|e| io::Error::new(e.kind(), format!("open {lock_path}: {e}")))?;
        let _ = f.set_permissions(std::fs::Permissions::from_mode(0o600));
        if unsafe { flock(std::os::unix::io::AsRawFd::as_raw_fd(&f), LOCK_EX | LOCK_NB) } == 0 {
            break f;
        }
        let e = io::Error::last_os_error();
        if e.raw_os_error() == Some(libc_eagain()) {
            let foreign = f
                .metadata()
                .map(|m| m.uid() != unsafe { geteuid() })
                .unwrap_or(false);
            if foreign && retry_foreign {
                retry_foreign = false;
                drop(f);
                let _ = std::fs::remove_file(&lock_path);
                continue;
            }
            return Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                format!("another sqlw service already owns {endpoint} (lock held)"),
            ));
        }
        return Err(io::Error::new(e.kind(), format!("flock {lock_path}: {e}")));
    };

    // Create the socket with owner-only mode: umask around bind, chmod as a
    // belt-and-braces (the existing set_socket_owner_only also covers the
    // stale-socket rebind path below).
    let old_umask = unsafe { umask(0o177 as RawMode) };
    let bind_result = std::os::unix::net::UnixListener::bind(endpoint);
    unsafe { umask(old_umask) };

    let listener = match bind_result {
        Ok(l) => l,
        Err(e) if e.kind() == io::ErrorKind::AddrInUse => {
            match std::os::unix::net::UnixStream::connect(endpoint) {
                Ok(_) => {
                    // Reachable but its owner died without releasing? A live
                    // service would hold the flock we already hold — so a
                    // connectable socket here means a foreign process.
                    return Err(io::Error::new(
                        io::ErrorKind::AddrInUse,
                        format!("a foreign process is listening on {endpoint}; refusing"),
                    ));
                }
                // A live service owned by another user: refuse rather than steal.
                Err(e2) if e2.kind() == io::ErrorKind::PermissionDenied => {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        format!("{endpoint} is owned by another service; refusing to remove it"),
                    ));
                }
                // Stale socket from a crashed process: safe to replace — but
                // only when the path really is a socket and nothing listens.
                Err(e2) if e2.kind() == io::ErrorKind::ConnectionRefused => {
                    let is_socket = std::fs::symlink_metadata(endpoint)
                        .map(|m| m.file_type().is_socket())
                        .unwrap_or(false);
                    if !is_socket {
                        return Err(io::Error::new(
                            io::ErrorKind::AddrInUse,
                            format!("{endpoint} exists but is not a socket; refusing to remove it"),
                        ));
                    }
                    std::fs::remove_file(endpoint)?;
                    let old_umask = unsafe { umask(0o177 as RawMode) };
                    let l = std::os::unix::net::UnixListener::bind(endpoint);
                    unsafe { umask(old_umask) };
                    l?
                }
                // Any other probe failure: do not delete what we don't understand.
                Err(e2) => {
                    return Err(io::Error::new(
                        e2.kind(),
                        format!("cannot probe existing endpoint {endpoint}: {e2}"),
                    ));
                }
            }
        }
        Err(e) => return Err(e),
    };
    set_socket_owner_only(endpoint)?;

    Ok(Listener {
        inner: listener,
        _lock: lock,
        #[cfg(target_os = "linux")]
        self_uid: unsafe { geteuid() },
    })
}

#[cfg(unix)]
fn libc_eagain() -> i32 {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        11
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        35
    }
}

#[cfg(unix)]
fn set_socket_owner_only(path: &str) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
}

#[cfg(unix)]
impl Listener {
    pub fn accept(&mut self) -> io::Result<Box<dyn Conn>> {
        loop {
            let (stream, _) = self.inner.accept()?;
            // Linux: SO_PEERCRED identity check — only the service's own uid.
            // ponytail: macOS needs LOCAL_PEERCRED/xucred instead, untestable
            // here; upgrade path: add the macOS variant when a build exists.
            #[cfg(target_os = "linux")]
            {
                match peer_uid(&stream) {
                    Some(uid) if uid == self.self_uid => {}
                    other => {
                        eprintln!(
                            "sqlw: rejected unix connection from uid {} (service uid {})",
                            other.map_or_else(|| "unknown".into(), |u| u.to_string()),
                            self.self_uid
                        );
                        continue;
                    }
                }
            }
            return Ok(Box::new(stream));
        }
    }
}

#[cfg(target_os = "linux")]
fn peer_uid(stream: &std::os::unix::net::UnixStream) -> Option<u32> {
    #[repr(C)]
    struct UCred {
        pid: i32,
        uid: u32,
        gid: u32,
    }
    const SOL_SOCKET: i32 = 1;
    const SO_PEERCRED: i32 = 17;
    extern "C" {
        fn getsockopt(
            sockfd: i32,
            level: i32,
            optname: i32,
            optval: *mut core::ffi::c_void,
            optlen: *mut u32,
        ) -> i32;
    }
    let mut cred = UCred {
        pid: 0,
        uid: 0,
        gid: 0,
    };
    let mut len = std::mem::size_of::<UCred>() as u32;
    let rc = unsafe {
        getsockopt(
            std::os::unix::io::AsRawFd::as_raw_fd(stream),
            SOL_SOCKET,
            SO_PEERCRED,
            &mut cred as *mut _ as *mut _,
            &mut len,
        )
    };
    (rc == 0).then_some(cred.uid)
}

#[cfg(windows)]
pub fn connect(endpoint: &str) -> io::Result<Box<dyn Conn>> {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(pipe_name(endpoint))
        .map(|s| Box::new(s) as Box<dyn Conn>)
}

#[cfg(unix)]
pub fn connect(endpoint: &str) -> io::Result<Box<dyn Conn>> {
    std::os::unix::net::UnixStream::connect(endpoint).map(|s| Box::new(s) as Box<dyn Conn>)
}
