package io.tapdata.modules.api.net.service.node.connection;

import java.util.function.BiFunction;

public interface NodeConnectionFactory {
	NodeConnection getNodeConnection(String nodeId);

	boolean isDisconnected(String nodeId);

	NodeConnection removeNodeConnection(String nodeId);

	<Request, Response> void registerReceiver(String type, Receiver<Response, Request> receiver);

	Object received(String nodeId, String type, Byte encode, byte[] data);
}