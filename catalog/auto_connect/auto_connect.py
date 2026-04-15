import cv2
import numpy as np
from PIL import ImageGrab
from pyautogui import moveTo, click
from time import sleep
from threading import Thread, Event
from pynput import mouse, keyboard

# Pause control flags
pause_event = Event()
cooldown_event = Event()

# Trigger pause without blocking the listener thread
def trigger_pause():
    if cooldown_event.is_set():
        return

    print("User activity detected. Pausing automation for 60 seconds...")
    pause_event.set()
    cooldown_event.set()

    def resume():
        sleep(60)
        print("Resuming automation...")
        pause_event.clear()
        cooldown_event.clear()

    Thread(target=resume, daemon=True).start()

# Listener callbacks
def on_mouse_move(x, y): trigger_pause()
def on_click(x, y, button, pressed): trigger_pause()
def on_scroll(x, y, dx, dy): trigger_pause()
def on_key_press(key): trigger_pause()

# Start input listeners
def start_listeners():
    with mouse.Listener(on_move=on_mouse_move, on_click=on_click, on_scroll=on_scroll) as m_listener, \
         keyboard.Listener(on_press=on_key_press) as k_listener:
        m_listener.join()
        k_listener.join()

Thread(target=start_listeners, daemon=True).start()

# Fast image detection using OpenCV
def locate_button_fast(template_path='button.png', threshold=0.8):
    try:
        screen = np.array(ImageGrab.grab())
        screen_gray = cv2.cvtColor(screen, cv2.COLOR_BGR2GRAY)
        template = cv2.imread(template_path, cv2.IMREAD_GRAYSCALE)

        if template is None:
            print("Error: Could not load button image.")
            return None

        res = cv2.matchTemplate(screen_gray, template, cv2.TM_CCOEFF_NORMED)
        _, max_val, _, max_loc = cv2.minMaxLoc(res)

        if max_val >= threshold:
            h, w = template.shape
            return (max_loc[0] + w // 2, max_loc[1] + h // 2)
        return None
    except Exception as e:
        print(f"Template matching failed: {e}")
        return None

# Automation action
def run():
    moveTo(100, 100)
    moveTo(200, 100)

    location = locate_button_fast('button.png')
    if location:
        print("Button found. Clicking...")
        moveTo(location)
        click()
    else:
        print("Button not found.")

# Main loop
try:
    while True:
        if pause_event.is_set():
            sleep(1)
            continue

        run()
        sleep(10)  # reduce load
except KeyboardInterrupt:
    print("Script terminated by user.")
