from __future__ import annotations

import base64
from pathlib import Path

import requests


# Ollama-maskinen på lokalnettet
OLLAMA_URL = "http://192.168.10.172:11434/api/chat"

# Endre denne dersom `ollama list` viser et annet modellnavn.
MODEL = "qwen3-vl:8b"

SUPPORTED_EXTENSIONS = {
    ".jpg",
    ".jpeg",
    ".png",
    ".webp",
    ".bmp",
    ".gif",
}

PROMPT = """
Analyze the image carefully and objectively.

Provide the following sections:

1. Image overview
Describe what is directly visible in the image.

2. Important objects
List the important objects, people, buildings, or user-interface elements.

3. Visible text
Transcribe all readable text exactly.
If no text is visible, state that briefly.

4. Errors or problems
Report any visible error messages, warnings, damaged elements, unusual states,
or possible problems.
Do not claim that no hidden or technical problems exist. Only describe what
can be determined visually.

5. What appears to be happening
Describe the visible activity or state of the scene.

6. Uncertainty
List anything that is unclear, partially visible, or based on interpretation.

Rules:
- Write in clear and concise English.
- Describe only what can reasonably be observed.
- Separate observations from interpretations.
- Do not infer emotions, intentions, ownership, location, building condition,
  or technical condition without visible evidence.
- Do not say that there is no uncertainty unless this is genuinely justified.
- Do not invent details.
"""

def encode_image(image_path: Path) -> str:
    """Les bildet og returner det Base64-kodet."""
    return base64.b64encode(image_path.read_bytes()).decode("utf-8")


def analyze_image(image_path: Path) -> str:
    """Send ett bilde til Qwen3-VL gjennom Ollama."""
    payload = {
        "model": MODEL,
        "messages": [
            {
                "role": "user",
                "content": PROMPT,
                "images": [encode_image(image_path)],
            }
        ],
        "stream": False,
        "options": {
            "temperature": 0.2,
        },
    }

    response = requests.post(
        OLLAMA_URL,
        json=payload,
        timeout=600,
    )
    response.raise_for_status()

    data = response.json()
    return data["message"]["content"].strip()


def main() -> None:
    # Bruk mappen der selve scriptet ligger.
    script_directory = Path(__file__).resolve().parent

    image_paths = sorted(
        path
        for path in script_directory.iterdir()
        if path.is_file() and path.suffix.lower() in SUPPORTED_EXTENSIONS
    )

    if not image_paths:
        print(f"Fant ingen bilder i: {script_directory}")
        return

    output_path = script_directory / "bildebeskrivelser.md"

    report_parts = [
        "# Bildebeskrivelser",
        "",
        f"Modell: `{MODEL}`",
        "",
    ]

    print(f"Fant {len(image_paths)} bilde(r).")
    print(f"Bruker modell: {MODEL}")
    print()

    for number, image_path in enumerate(image_paths, start=1):
        print(f"[{number}/{len(image_paths)}] Analyserer {image_path.name} ...")

        try:
            explanation = analyze_image(image_path)
        except requests.RequestException as error:
            explanation = f"Kunne ikke analysere bildet: {error}"
            print(f"  Feil: {error}")
        except (KeyError, ValueError) as error:
            explanation = f"Uventet svar fra Ollama: {error}"
            print(f"  Uventet svar: {error}")
        else:
            print("  Ferdig.")

        report_parts.extend(
            [
                f"## {image_path.name}",
                "",
                explanation,
                "",
                "---",
                "",
            ]
        )

        # Oppdater rapporten etter hvert bilde, slik at resultater ikke går
        # tapt dersom scriptet blir avbrutt.
        output_path.write_text(
            "\n".join(report_parts),
            encoding="utf-8",
        )

    print()
    print(f"Alle beskrivelser er lagret i:")
    print(output_path)


if __name__ == "__main__":
    main()