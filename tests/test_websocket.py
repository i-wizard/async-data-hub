from tests.fixtures import WEBSOCKET_UPDATES_URL


def test_websocket_connects_receives_broadcast_and_cleans_up(client):
    """
    Verifies the WebSocket manager can broadcast to a live client and removes
    the client from the registry after the socket closes.
    """

    with client.websocket_connect(WEBSOCKET_UPDATES_URL) as websocket:
        client.portal.call(
            client.app.state.container.websocket_manager.broadcast,
            '{"event":"test","payload":"ok"}',
        )
        message = websocket.receive_text()
        assert '"event":"test"' in message

    active_clients = client.portal.call(
        client.app.state.container.websocket_manager.active_client_ids,
    )
    assert active_clients == []
