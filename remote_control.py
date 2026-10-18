import paho.mqtt.client as mqtt
import os
import sys
import tty
import termios

MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto")
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.connect(MQTT_HOST, 1883)

def getch():
    """Legge un singolo carattere dalla tastiera senza invio"""
    fd = sys.stdin.fileno()
    old_settings = termios.tcgetattr(fd)
    try:
        tty.setraw(sys.stdin.fileno())
        ch = sys.stdin.read(1)
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
    return ch

print("""
🎮 CONTROLLO REMOTO ATTIVO
---------------------------
W: Avanti | S: Indietro
A: Sinistra | D: Destra
Spazio: STOP | Q: Esci
---------------------------
""")
client.publish("robot/commands", "HELLO")
try:
    while True:
        char = getch().lower()
        
        if char == 'w':
            print("Avanti")
            client.publish("robot/commands", "FORWARD")
        elif char == 's':
            print("Indietro")
            client.publish("robot/commands", "BACKWARD")
        elif char == 'a':
            print("Sinistra")
            client.publish("robot/commands", "LEFT")
        elif char == 'd':
            print("Destra")
            client.publish("robot/commands", "RIGHT")
        elif char == ' ':
            print("STOP")
            client.publish("robot/commands", "STOP")
        elif char == 'q':
            print("\nChiusura...")
            break
except KeyboardInterrupt:
    pass
finally:
    client.disconnect()