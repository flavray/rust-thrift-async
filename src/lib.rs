mod transport;

use std::net::SocketAddr;
use std::sync::Arc;

use failure::Error;
use futures::try_ready;
use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol};
use thrift::server::TProcessor;
use tokio::net::TcpListener;
use tokio::prelude::*;

use crate::transport::{ReadResult, TAReadFramedTransport, TAWriteFramedTransport};

pub const DEFAULT_MAX_FRAME_SIZE: u32 = 256 * 1024 * 1024;
pub const DEFAULT_CORE_READ_FRAME_SIZE: u32 = 2 * 1024 * 1024;
pub const DEFAULT_CORE_WRITE_FRAME_SIZE: u32 = 2 * 1024 * 1024;
pub const DEFAULT_CORE_RESIZE_FREQUENCY: u64 = 512;

#[derive(Clone, Copy, Debug)]
struct CoreResize {
    read_size: usize,
    write_size: usize,
    frequency: u64,
}

struct TAsyncProcessor<R: AsyncRead, W: AsyncWrite, P: TProcessor> {
    reader: R,
    writer: W,
    processor: Arc<P>,
    max_frame_size: usize,
    core_resize: CoreResize,
}

impl<R: AsyncRead, W: AsyncWrite, P: TProcessor> TAsyncProcessor<R, W, P> {
    fn new(reader: R, writer: W, processor: Arc<P>, max_frame_size: usize, core_resize: CoreResize) -> Self {
        TAsyncProcessor {
            reader,
            writer,
            processor,
            max_frame_size,
            core_resize,
        }
    }
}

impl<R: AsyncRead, W: AsyncWrite, P: TProcessor> Future for TAsyncProcessor<R, W, P> {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<<Self as Future>::Item>, <Self as Future>::Error> {
        let max_frame_size = self.max_frame_size as usize;
        let mut read_transport = TAReadFramedTransport::new(&mut self.reader, max_frame_size);

        let mut write_transport = TAWriteFramedTransport::new(&mut self.writer);

        let mut processed_count = 0u64;

        loop {
            match try_ready!(read_transport.poll()) {
                ReadResult::Ok => (),
                ReadResult::EOF => break, // EOF reached - client disconnected
                ReadResult::TooLarge(size) => { // Frame is too large (non-thrift framed input?) - disconnect client
                    eprintln!("Frame size {} too large (maximum: {})", size, self.max_frame_size);
                    break
                },
            }

            let read_cursor = read_transport.frame_cursor();

            {
                // TODO: I want NLL :(
                let write_cursor = write_transport.frame_cursor();

                let mut input_protocol = TBinaryInputProtocol::new(read_cursor, false);
                let mut output_protocol = TBinaryOutputProtocol::new(write_cursor, false);

                let process = self
                    .processor
                    .process(&mut input_protocol, &mut output_protocol);

                if let Err(e) = process {
                    eprintln!("Error processing thrift input - {:?}", e);
                }
            }

            try_ready!(write_transport.poll());

            // resize core frame sizes
            processed_count += 1;
            if processed_count % self.core_resize.frequency == 0 {
                read_transport.core_resize(self.core_resize.read_size);
                write_transport.core_resize(self.core_resize.write_size);
            }
        }

        Ok(Async::Ready(()))
    }
}

pub struct TAsyncServer<P: TProcessor + Send + Sync + 'static> {
    processor: Arc<P>,
    max_frame_size: u32,
    core_read_frame_size: u32,
    core_write_frame_size: u32,
    core_resize_frequency: u64,
}

impl<P: TProcessor + Send + Sync + 'static> TAsyncServer<P> {
    pub fn new(processor: P) -> TAsyncServer<P> {
        TAsyncServer {
            processor: Arc::new(processor),
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            core_read_frame_size: DEFAULT_CORE_READ_FRAME_SIZE,
            core_write_frame_size: DEFAULT_CORE_WRITE_FRAME_SIZE,
            core_resize_frequency: DEFAULT_CORE_RESIZE_FREQUENCY,
        }
    }

    pub fn max_frame_size(&mut self, max_frame_size: u32) -> &mut Self {
        self.max_frame_size = max_frame_size;
        self
    }

    pub fn core_read_frame_size(&mut self, core_read_frame_size: u32) -> &mut Self {
        self.core_read_frame_size = core_read_frame_size;
        self
    }

    pub fn core_write_frame_size(&mut self, core_write_frame_size: u32) -> &mut Self {
        self.core_write_frame_size = core_write_frame_size;
        self
    }

    pub fn core_resize_frequency(&mut self, core_resize_frequency: u64) -> &mut Self {
        self.core_resize_frequency = core_resize_frequency;
        self
    }

    pub fn listen(&mut self, address: &str) -> Result<(), Error> {
        let address = address.parse::<SocketAddr>()?;

        self.listen_address(address)
    }

    pub fn listen_address(&mut self, address: SocketAddr) -> Result<(), Error> {
        let socket = TcpListener::bind(&address)?;

        let processor = self.processor.clone();
        let max_frame_size = self.max_frame_size as usize;
        let core_resize = CoreResize {
            read_size: self.core_read_frame_size as usize,
            write_size: self.core_write_frame_size as usize,
            frequency: self.core_resize_frequency,
        };

        let server = socket
            .incoming()
            .map_err(|err| eprintln!("Socket Error: {:?}", err))
            .for_each(move |socket| {
                let (reader, writer) = socket.split();
                let processor = processor.clone();
                let async_processor =
                    TAsyncProcessor::new(reader, writer, processor, max_frame_size, core_resize);

                tokio::spawn(async_processor)
            });

        tokio::run(server);

        Ok(())

        /*
        TODO: one day.
        self.runtime
            .spawn(server);
        
        self.runtime
            .shutdown_on_idle()
            .wait()
            .unwrap();  // TODO: error
        */
    }
}
