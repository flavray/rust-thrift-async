use std::io::Cursor;
use std::mem::transmute;

use tokio::prelude::*;

pub enum ReadResult {
    Ok,
    EOF,
    TooLarge(usize),
}

pub struct TAReadFramedTransport<'a, R: 'a + AsyncRead> {
    read: &'a mut R,
    frame_buffer: Vec<u8>,
    max_frame_size: usize,
}

impl<'a, R: AsyncRead> TAReadFramedTransport<'a, R> {
    pub fn new(read: &'a mut R, max_frame_size: usize) -> Self {
        TAReadFramedTransport {
            read,
            frame_buffer: Vec::new(),
            max_frame_size,
        }
    }
}

impl<'a, R: AsyncRead> TAReadFramedTransport<'a, R> {
    pub fn frame_cursor(&self) -> Cursor<&Vec<u8>> {
        Cursor::new(&self.frame_buffer)
    }
}

impl<'a, R: 'a + AsyncRead> Future for TAReadFramedTransport<'a, R> {
    type Item = ReadResult;
    type Error = ();

    fn poll(&mut self) -> Result<Async<<Self as Future>::Item>, <Self as Future>::Error> {
        let mut frame_size_buffer = [0u8, 0u8, 0u8, 0u8];

        let n = try_ready!(
            self.read
                .poll_read(&mut frame_size_buffer)
                .map_err(|e| println!("{:?}", e))
        );

        // EOF reached - client disconnected
        if n == 0 {
            return Ok(Async::Ready(ReadResult::EOF))
        }

        let frame_size = unsafe { transmute::<[u8; 4], u32>(frame_size_buffer) };
        let frame_size = u32::from_be(frame_size) as usize;

        // TODO: maximum frame size check
        if frame_size > self.max_frame_size {
            return Ok(Async::Ready(ReadResult::TooLarge(frame_size)))
        }

        self.frame_buffer.resize(frame_size, 0u8);

        self.read
            .read_exact(&mut self.frame_buffer)
            .map_err(|e| println!("{:?}", e))?;

        Ok(Async::Ready(ReadResult::Ok))
    }
}

pub struct TAWriteFramedTransport<'a, W: 'a + AsyncWrite> {
    write: &'a mut W,
    frame_buffer: Vec<u8>,
}

impl<'a, W: AsyncWrite> TAWriteFramedTransport<'a, W> {
    pub fn new(write: &'a mut W) -> TAWriteFramedTransport<'a, W> {
        TAWriteFramedTransport {
            write,
            frame_buffer: Vec::with_capacity(4),
        }
    }
}

impl<'a, W: AsyncWrite> TAWriteFramedTransport<'a, W> {
    pub fn frame_cursor(&mut self) -> Cursor<&mut Vec<u8>> {
        let mut cursor = Cursor::new(&mut self.frame_buffer);
        cursor.set_position(4); // leave 4 bytes to write frame size
        cursor
    }

    fn is_frame_empty(&self) -> bool {
        self.frame_buffer.len() <= 4
    }

    /// Write the frame size at the beginning of the frame buffer
    /// This allows to write to sockets only once (in most cases).
    fn set_frame_size(&mut self) {
        let frame_size = (self.frame_buffer.len() - 4) as u32;

        let output_frame_size = frame_size.to_be();
        let output_frame_size_buffer = unsafe { transmute::<u32, [u8; 4]>(output_frame_size) };

        self.frame_buffer[..4].copy_from_slice(&output_frame_size_buffer);
    }
}

impl<'a, W: 'a + AsyncWrite> Future for TAWriteFramedTransport<'a, W> {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<<Self as Future>::Item>, <Self as Future>::Error> {
        if self.is_frame_empty() {
            return Ok(Async::Ready(()))
        }

        self.set_frame_size();

        self.write
            .write_all(&self.frame_buffer)
            .map_err(|e| println!("{:?}", e))?;

        Ok(Async::Ready(()))
    }
}
