"""
Desktop automation utility with user-activity pause protection.

This script repeatedly looks for a button image on the screen using OpenCV
template matching and clicks it when found. To reduce interference with normal
desktop use, any detected mouse or keyboard activity pauses the automation for
60 seconds before it resumes automatically.

Main behavior:
- monitors user mouse/keyboard activity in the background
- pauses automation whenever activity is detected
- periodically captures the screen and searches for a template image
- clicks the matched button location if confidence exceeds the threshold

Assumptions:
- the script runs in a desktop session with screen capture available
- the target button visually matches ``button.png`` closely enough for template matching
- screen scale, resolution, and theme do not differ so much that matching fails

Limitations:
- template matching is sensitive to visual changes such as scaling, color shifts,
  partial occlusion, or UI redesign
- any user activity triggers the same pause behavior, including small mouse movement
- automation is based on screen coordinates and visual state, not application semantics

This is a utility script for desktop automation, not a general-purpose UI
testing framework.
"""

from threading import Event, Thread
from time import sleep

import cv2
import numpy as np
from PIL import ImageGrab
from pyautogui import click, moveTo
from pynput import keyboard, mouse

# Event used to temporarily suspend automation after user input is detected.
pause_event = Event()

# Prevents repeated user-input events from spawning many overlapping cooldown threads.
cooldown_event = Event()


def trigger_pause():
    """
    Pause automation after detected user activity.

    If a cooldown is already active, additional user events are ignored. Otherwise,
    automation is paused for 60 seconds and then resumed automatically in a
    background thread.
    """
    if cooldown_event.is_set():
        return

    print("User activity detected. Pausing automation for 60 seconds...")
    pause_event.set()
    cooldown_event.set()

    def resume():
        """Clear pause/cooldown flags after the fixed inactivity timeout."""
        sleep(60)
        print("Resuming automation...")
        pause_event.clear()
        cooldown_event.clear()

    Thread(target=resume, daemon=True).start()


def on_mouse_move(x, y):
    """Pause automation when the user moves the mouse."""
    trigger_pause()


def on_click(x, y, button, pressed):
    """Pause automation when the user clicks the mouse."""
    trigger_pause()


def on_scroll(x, y, dx, dy):
    """Pause automation when the user scrolls."""
    trigger_pause()


def on_key_press(key):
    """Pause automation when the user presses a key."""
    trigger_pause()


def start_listeners():
    """
    Start background mouse and keyboard listeners.

    These listeners run continuously and trigger the shared pause logic whenever
    user input is detected.
    """
    with mouse.Listener(
        on_move=on_mouse_move,
        on_click=on_click,
        on_scroll=on_scroll,
    ) as m_listener, keyboard.Listener(on_press=on_key_press) as k_listener:
        m_listener.join()
        k_listener.join()


Thread(target=start_listeners, daemon=True).start()


def locate_button_fast(template_path="button.png", threshold=0.8):
    """
    Locate the center of a template image on the current screen.

    Parameters
    ----------
    template_path : str, default="button.png"
        Path to the template image to search for.
    threshold : float, default=0.8
        Minimum normalized template-match confidence required to accept a match.

    Returns
    -------
    tuple[int, int] | None
        Screen coordinates of the matched template center if found with
        sufficient confidence, otherwise None.

    Notes
    -----
    Matching is performed on grayscale images for speed and simplicity.
    """
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


def run():
    """
    Perform one automation cycle.

    The script first performs a small cursor movement, then tries to locate the
    target button on screen and clicks it if found.
    """
    # These movements may be intended to wake or nudge the UI before matching.
    # If that is not required, this behavior should be reconsidered explicitly.
    moveTo(100, 100)
    moveTo(200, 100)

    location = locate_button_fast("button.png")
    if location:
        print("Button found. Clicking...")
        moveTo(location)
        click()
    else:
        print("Button not found.")


try:
    while True:
        if pause_event.is_set():
            sleep(1)
            continue

        run()
        sleep(10)  # Polling interval chosen to reduce CPU/load between checks.

except KeyboardInterrupt:
    print("Script terminated by user.")