import threading
from database.sql_database import initialize_database, list_available_tables, Tables
from cloud.network_manager import check_network, has_ip_address, Internet_connected
# from protocol_ingestor.protocol_ingestor import start_ingestor
# from cloud.mqtt_uploader import start_mqtt_connection




def main():
    initialize_database()
    Tables = list_available_tables()
    print("Available tables:", Tables)


    # check network status
    network_status = check_network()
    print(f"Wi-Fi connected: {network_status['wifi_connected']}, Ethernet connected: {network_status['ethernet_connected']}, Internet accessible: {network_status['internet_connected']}")
    if network_status['wifi_connected'] or network_status['ethernet_connected']:
        if has_ip_address("wlan0") or has_ip_address("eth0"):
            print("Device has an IP address.")
            Internet_connected = True
        else:
            print("Device does not have an IP address.")
        
    

    if Internet_connected:
        print("Device is connected to the internet.")
        start_mqtt_connection()


    # ingestor_thread = threading.Thread(
    #     target=start_ingestor,
    #     name="protocol-ingestor",
    #     daemon=True
    # )

    # mqtt_thread = threading.Thread(
    #     target=start_mqtt_uploader,
    #     name="mqtt-uploader",
    #     daemon=True
    # )

    

    # ingestor_thread.start()
    # mqtt_thread.start()

    
    # ingestor_thread.join()
    # mqtt_thread.join()

    # threading.Thread(target=heartbeat, daemon=True).start()


if __name__ == "__main__":
    main()