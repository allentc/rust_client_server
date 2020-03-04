// A workaround to obtain a nonblocking tokio UnixStream with an 'abstract'
// name.  This is necessary because mio_uds eats the EAGAIN result to connect()
// that occurs when you open large numbers of AF_UNIX sockets very quickly.
// Also, std::os::unix::net::UnixStream doesn't permit abstract names.
//
// So, a sockaddr_un is prepared with an abstract path.  Then libc is used to
// obtain a raw file descriptor from the OS.  The abstract name is then used to
// perform a non-blocking connect().  The now connected raw file descriptor is
// then used to make a std UnixStream which is then used to make a tokio non-
// blocking UnixStream.

use std::io;
use std::mem::{size_of, zeroed};
use std::os::unix::io::FromRawFd;
use std::slice::from_raw_parts_mut;
use tokio::net::UnixStream;

pub async fn connect(path: &str) -> io::Result<UnixStream> {
    let (addr, len) = make_sockaddr_un_from_path(path);
    let fh = socket_()?;
    let addr = &addr as *const _ as *const _;
    connect_(fh, addr, len)?;
    let stream = unsafe { std::os::unix::net::UnixStream::from_raw_fd(fh) };
    UnixStream::from_std(stream)
}

fn make_sockaddr_un_from_path(path: &str) -> (libc::sockaddr_un, libc::socklen_t) {
    let offset = size_of::<libc::sa_family_t>();
    let bytes = path.as_bytes();
    let len = (offset + bytes.len()) as u32;
    let mut addr: libc::sockaddr_un = unsafe { zeroed() };
    addr.sun_family = libc::AF_UNIX as libc::sa_family_t;
    let ptr = &addr as *const _ as *mut u8;
    let ptr = unsafe { ptr.add(offset) };
    let sun_path = unsafe { from_raw_parts_mut(ptr, bytes.len()) };
    sun_path.copy_from_slice(bytes);

    (addr, len)
}

fn check_err<T: Ord + Default>(num: T) -> io::Result<T> {
    if num < T::default() {
        return Err(io::Error::last_os_error());
    }
    Ok(num)
}

fn socket_() -> io::Result<i32> {
    check_err(unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0) })
}

fn connect_(
    socket: libc::c_int,
    address: *const libc::sockaddr,
    len: libc::socklen_t,
) -> io::Result<()> {
    check_err(unsafe { libc::connect(socket, address, len) })?;
    Ok(())
}
