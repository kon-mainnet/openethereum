use ansi_term::Colour;
use host::Host;
use io::*;
use network::{
	ConnectionFilter, Error, NetworkConfiguration, NetworkContext, NetworkIoMessage,
	NetworkProtocolHandler, NonReservedPeerMode, PeerId, ProtocolId,
};
use once_cell::sync::OnceCell;
use std::{net::SocketAddr, ops::RangeInclusive, sync::Arc};

struct HostHandler {
	public_url: OnceCell<String>,
}

impl IoHandler<NetworkIoMessage> for HostHandler {
	fn message(&self, _io: &IoContext<NetworkIoMessage>, message: &NetworkIoMessage) {
		if let NetworkIoMessage::NetworkStarted(ref public_url) = *message {
			if self.public_url.get().map_or(true, |uref| uref != public_url) {
				info!(
                    target: "network",
                    "Public node URL: {}",
                    Colour::White.bold().paint(mask_url(AsRef::<str>::as_ref(public_url)))
                );
				let _ = self.public_url.set(public_url.to_owned());
			}
		}
	}
}

fn mask_url(url: &str) -> String {
	let len = url.len().min(20);
	format!("{}...", &url[..len])
}

pub struct NetworkService {
	io_service: IoService<NetworkIoMessage>,
	host_info: String,
	host: OnceCell<Arc<Host>>,
	host_handler: Arc<HostHandler>,
	config: NetworkConfiguration,
	filter: Option<Arc<dyn ConnectionFilter>>,
}

impl NetworkService {
	pub fn new(
		config: NetworkConfiguration,
		filter: Option<Arc<dyn ConnectionFilter>>,
	) -> Result<NetworkService, Error> {
		let host_handler = Arc::new(HostHandler {
			public_url: OnceCell::new(),
		});
		let io_service = IoService::<NetworkIoMessage>::start("devp2p")?;

		Ok(NetworkService {
			io_service,
			host_info: config.client_version.clone(),
			host: OnceCell::new(),
			config,
			host_handler,
			filter,
		})
	}

	pub fn register_protocol(
		&self,
		handler: Arc<dyn NetworkProtocolHandler + Send + Sync>,
		protocol: ProtocolId,
		versions: &[(u8, u8)],
	) -> Result<(), Error> {
		self.io_service.send_message(NetworkIoMessage::AddHandler {
			handler,
			protocol,
			versions: versions.to_vec(),
		})?;
		Ok(())
	}

	pub fn host_info(&self) -> String {
		self.host_info.clone()
	}

	pub fn io(&self) -> &IoService<NetworkIoMessage> {
		&self.io_service
	}

	pub fn num_peers_range(&self) -> RangeInclusive<u32> {
		self.config.min_peers..=self.config.max_peers
	}

	pub fn external_url(&self) -> Option<String> {
		self.host.get().and_then(|h| h.external_url())
	}

	pub fn local_url(&self) -> Option<String> {
		self.host.get().map(|h| h.local_url())
	}

	pub fn start(&self) -> Result<(), (Error, Option<SocketAddr>)> {
		let listen_addr = self.config.listen_address;
		if self.host.get().is_none() {
			let h = Arc::new(
				Host::new(self.config.clone(), self.filter.clone())
					.map_err(|err| (err, listen_addr))?,
			);
			self.io_service
				.register_handler(h.clone())
				.map_err(|err| (err.into(), listen_addr))?;
			self.host.set(h).map_err(|_| (Error::Internal("Host already initialized".into()), listen_addr))?;
		}

		if self.host_handler.public_url.get().is_none() {
			self.io_service
				.register_handler(self.host_handler.clone())
				.map_err(|err| (err.into(), listen_addr))?;
		}

		Ok(())
	}

	pub fn stop(&self) {
		if let Some(ref host) = self.host.get() {
			let token = host.token().unwrap_or(0);
			let io = IoContext::new(self.io_service.channel(), token);
			host.stop(&io);
		}
	}

	pub fn connected_peers(&self) -> Vec<PeerId> {
		self.host
			.get()
			.map(|h| h.connected_peers())
			.unwrap_or_default()
	}

	pub fn add_reserved_peer(&self, peer: &str) -> Result<(), Error> {
		if let Some(ref host) = self.host.get() {
			host.add_reserved_node(peer)
		} else {
			Ok(())
		}
	}

	pub fn remove_reserved_peer(&self, peer: &str) -> Result<(), Error> {
		if let Some(ref host) = self.host.get() {
			host.remove_reserved_node(peer)
		} else {
			Ok(())
		}
	}

	pub fn set_non_reserved_mode(&self, mode: NonReservedPeerMode) {
		if let Some(ref host) = self.host.get() {
			let token = host.token().unwrap_or(0);
			let io_ctxt = IoContext::new(self.io_service.channel(), token);
			host.set_non_reserved_mode(mode, &io_ctxt);
		}
	}

	pub fn with_context<F>(&self, protocol: ProtocolId, action: F)
	where
		F: FnOnce(&dyn NetworkContext),
	{
		if let Some(ref host) = self.host.get() {
			let token = host.token().unwrap_or(0);
			let io = IoContext::new(self.io_service.channel(), token);
			host.with_context(protocol, &io, action);
		}
	}

	pub fn with_context_eval<F, T>(&self, protocol: ProtocolId, action: F) -> Option<T>
	where
		F: FnOnce(&dyn NetworkContext) -> T,
	{
		self.host.get().map(|host| {
			let token = host.token().unwrap_or(0);
			let io = IoContext::new(self.io_service.channel(), token);
			host.with_context_eval(protocol, &io, action)
		})
	}
}
