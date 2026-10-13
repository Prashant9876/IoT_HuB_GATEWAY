
import socket
from pathlib import Path

Internet_connected = False 


def is_interface_up(interface: str) -> bool:
    """Returns True when Linux reports the interface as active."""
    state_file = Path(f"/sys/class/net/{interface}/operstate")
    try:
        return state_file.read_text().strip() == "up"
    except FileNotFoundError:
        return False


def has_ip_address(interface: str) -> bool:
    """Checks whether the interface has an IPv4 address."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            # Connecting UDP does not send data; it identifies the route/IP used.
            sock.connect(("8.8.8.8", 53))
            local_ip = sock.getsockname()[0]
            print(f"Local IP address for {interface}: {local_ip}")
        return local_ip not in ("", "127.0.0.1")
    except OSError:
        return False


def check_network():
    wifi_connected = is_interface_up("wlan0")
    ethernet_connected = is_interface_up("eth0")

    # Tests actual internet/DNS access, not merely Wi-Fi association.
    internet_connected = False
    try:
        socket.create_connection(("1.1.1.1", 443), timeout=5).close()
        internet_connected = True
    except OSError:
        pass

    return {
        "wifi_connected": wifi_connected,
        "ethernet_connected": ethernet_connected,
        "internet_connected": internet_connected,
    }

