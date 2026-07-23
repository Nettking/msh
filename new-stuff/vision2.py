from __future__ import annotations

import base64
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

OLLAMA_BASE_URL = "http://192.168.10.172:11434"
OLLAMA_CHAT_URL = f"{OLLAMA_BASE_URL}/api/chat"

VISION_MODEL = "qwen3-vl:8b"

# Images are read from the folder where this script is located.
SCRIPT_DIRECTORY = Path(__file__).resolve().parent
IMAGE_DIRECTORY = SCRIPT_DIRECTORY

OUTPUT_FILE = SCRIPT_DIRECTORY / "bildebeskrivelser_refined.md"

SUPPORTED_EXTENSIONS = {
    ".png",
    ".jpg",
    ".jpeg",
    ".webp",
    ".bmp",
}

REQUEST_TIMEOUT_SECONDS = 1200
MODEL_KEEP_ALIVE = "10m"


# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

VISION_PROMPT = """
Analyze the supplied image carefully and objectively.

Return the analysis using exactly these six sections:

### 1. Image overview
Provide a concise overview of what is directly visible.

### 2. Important objects
List the important visible objects, people, buildings, or user-interface
elements. Include relevant spatial relationships when they are clear.

### 3. Visible text
Transcribe all readable text exactly.
If no readable text is visible, state that briefly.

### 4. Errors or problems
Report visible error messages, warnings, damage, unusual states, or other
possible problems.

Do not claim that hidden technical, mechanical, or structural problems do not
exist. Only report what can be visually assessed.

If no visible problems are apparent, write:
"No visible errors, warnings, or obvious abnormalities are apparent."

### 5. What appears to be happening
Describe the visible activity or current state of the scene.

### 6. Uncertainty
List only uncertainties that materially affect the interpretation.
If nothing important is uncertain, state that briefly.

Rules:
- Write in concise, professional English.
- Describe only what is visually supported.
- Separate observations from interpretations.
- Do not invent details.
- Do not infer emotions, intentions, ownership, location, safety, structural
  integrity, or technical condition without visible evidence.
- Be careful with colors, object positions, quantities, and spatial
  relationships.
- Do not add generic uncertainty about exact dimensions, ownership, hidden
  internal components, or information the image was never expected to show.
- Return only the six-section analysis.
""".strip()


REVIEW_PROMPT_TEMPLATE = """
You are performing a second visual quality-control pass.

You have been given:

1. The original image.
2. A draft description produced during an earlier vision-model pass.

Compare every statement in the draft against the original image.

Correct the description using exactly these six sections:

### 1. Image overview
### 2. Important objects
### 3. Visible text
### 4. Errors or problems
### 5. What appears to be happening
### 6. Uncertainty

Review requirements:
- Correct factual errors.
- Correct object positions and spatial relationships.
- Correct inaccurate colors, quantities, materials, and actions.
- Remove claims that are not clearly supported by the image.
- Remove unnecessary speculation.
- Remove false claims about structural or technical condition.
- Preserve accurate and useful observations.
- Use appropriate uncertainty when visual evidence is ambiguous.
- Keep the result concise.
- Do not mention that you are reviewing another model.
- Do not describe the review process.
- Do not add new details unless they are directly visible in the image.

For section 4:
- Report only visible errors, warnings, damage, unusual states, or obvious
  abnormalities.
- If none are apparent, write exactly:
  "No visible errors, warnings, or obvious abnormalities are apparent."

For section 6:
- Include only uncertainties that materially affect interpretation.
- Do not discuss exact dimensions, ownership, hidden components, or other
  unavailable information unless it directly affects the visual
  interpretation.

Return only the corrected six-section description.

Draft description:

---
{draft}
---
""".strip()


# ---------------------------------------------------------------------------
# Data structures and errors
# ---------------------------------------------------------------------------

class OllamaError(RuntimeError):
    """Raised when communication with Ollama fails."""


@dataclass
class OllamaResult:
    content: str
    total_duration_seconds: float | None
    load_duration_seconds: float | None
    prompt_tokens: int | None
    output_tokens: int | None


# ---------------------------------------------------------------------------
# Ollama communication
# ---------------------------------------------------------------------------

def encode_image(image_path: Path) -> str:
    """Read an image and return it as base64-encoded text."""

    try:
        image_bytes = image_path.read_bytes()
    except OSError as exc:
        raise OllamaError(
            f"Could not read image '{image_path.name}': {exc}"
        ) from exc

    return base64.b64encode(image_bytes).decode("utf-8")


def nanoseconds_to_seconds(value: Any) -> float | None:
    """Convert an Ollama nanosecond timing value to seconds."""

    if not isinstance(value, int):
        return None

    return value / 1_000_000_000


def call_ollama(
    *,
    model: str,
    messages: list[dict[str, Any]],
) -> OllamaResult:
    """Send a non-streaming chat request to Ollama."""

    payload = {
        "model": model,
        "messages": messages,
        "stream": False,
        "keep_alive": MODEL_KEEP_ALIVE,
        "options": {
            "temperature": 0.1,
        },
    }

    try:
        response = requests.post(
            OLLAMA_CHAT_URL,
            json=payload,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
    except requests.Timeout as exc:
        raise OllamaError(
            f"Ollama did not respond within "
            f"{REQUEST_TIMEOUT_SECONDS} seconds."
        ) from exc
    except requests.ConnectionError as exc:
        raise OllamaError(
            f"Could not connect to Ollama at {OLLAMA_BASE_URL}. "
            "Check the IP address, firewall, and Ollama network binding."
        ) from exc
    except requests.HTTPError as exc:
        response_text = response.text[:1000]

        raise OllamaError(
            f"Ollama returned HTTP {response.status_code}: "
            f"{response_text}"
        ) from exc
    except requests.RequestException as exc:
        raise OllamaError(f"Ollama request failed: {exc}") from exc

    try:
        data = response.json()
    except ValueError as exc:
        raise OllamaError(
            f"Ollama returned invalid JSON: {response.text[:1000]}"
        ) from exc

    message = data.get("message")

    if not isinstance(message, dict):
        raise OllamaError(
            f"Ollama response did not contain a valid message: {data}"
        )

    content = message.get("content")

    if not isinstance(content, str) or not content.strip():
        raise OllamaError(
            f"Ollama returned an empty response: {data}"
        )

    return OllamaResult(
        content=content.strip(),
        total_duration_seconds=nanoseconds_to_seconds(
            data.get("total_duration")
        ),
        load_duration_seconds=nanoseconds_to_seconds(
            data.get("load_duration")
        ),
        prompt_tokens=data.get("prompt_eval_count"),
        output_tokens=data.get("eval_count"),
    )


# ---------------------------------------------------------------------------
# Vision pipeline
# ---------------------------------------------------------------------------

def create_image_message(
    *,
    prompt: str,
    encoded_image: str,
) -> dict[str, Any]:
    """Create a message containing both text and an image."""

    return {
        "role": "user",
        "content": prompt,
        "images": [encoded_image],
    }


def analyze_image(
    encoded_image: str,
) -> OllamaResult:
    """Perform the first visual analysis pass."""

    message = create_image_message(
        prompt=VISION_PROMPT,
        encoded_image=encoded_image,
    )

    return call_ollama(
        model=VISION_MODEL,
        messages=[message],
    )


def review_description(
    *,
    encoded_image: str,
    draft: str,
) -> OllamaResult:
    """Review the draft while examining the original image again."""

    review_prompt = REVIEW_PROMPT_TEMPLATE.format(
        draft=draft
    )

    message = create_image_message(
        prompt=review_prompt,
        encoded_image=encoded_image,
    )

    return call_ollama(
        model=VISION_MODEL,
        messages=[message],
    )


# ---------------------------------------------------------------------------
# File handling
# ---------------------------------------------------------------------------

def find_images(directory: Path) -> list[Path]:
    """Find supported images in the given directory."""

    return sorted(
        (
            path
            for path in directory.iterdir()
            if path.is_file()
            and path.suffix.lower() in SUPPORTED_EXTENSIONS
        ),
        key=lambda path: path.name.lower(),
    )


def format_duration(
    duration: float | None,
) -> str:
    """Format a duration returned by Ollama."""

    if duration is None:
        return "not reported"

    return f"{duration:.1f} seconds"


def build_success_section(
    *,
    image_path: Path,
    draft_result: OllamaResult,
    final_result: OllamaResult,
    first_wall_time: float,
    second_wall_time: float,
) -> list[str]:
    """Build the Markdown output for one successful image."""

    total_wall_time = first_wall_time + second_wall_time

    return [
        f"## {image_path.name}",
        "",
        "### Final reviewed description",
        "",
        final_result.content,
        "",
        "<details>",
        "<summary>Show original first-pass description</summary>",
        "",
        draft_result.content,
        "",
        "</details>",
        "",
        "### Processing information",
        "",
        f"- First pass wall time: {first_wall_time:.1f} seconds",
        f"- Second pass wall time: {second_wall_time:.1f} seconds",
        f"- Total wall time: {total_wall_time:.1f} seconds",
        (
            "- First pass Ollama time: "
            f"{format_duration(draft_result.total_duration_seconds)}"
        ),
        (
            "- Second pass Ollama time: "
            f"{format_duration(final_result.total_duration_seconds)}"
        ),
        (
            "- First pass output tokens: "
            f"{draft_result.output_tokens
               if draft_result.output_tokens is not None
               else 'not reported'}"
        ),
        (
            "- Second pass output tokens: "
            f"{final_result.output_tokens
               if final_result.output_tokens is not None
               else 'not reported'}"
        ),
        "",
        "---",
        "",
    ]


def build_error_section(
    *,
    image_path: Path,
    error: Exception,
) -> list[str]:
    """Build the Markdown output for a failed image."""

    return [
        f"## {image_path.name}",
        "",
        "### Error",
        "",
        f"```text\n{error}\n```",
        "",
        "---",
        "",
    ]


def write_output(
    sections: list[str],
) -> None:
    """Write the current output to disk."""

    OUTPUT_FILE.write_text(
        "\n".join(sections),
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# Main program
# ---------------------------------------------------------------------------

def main() -> None:
    images = find_images(IMAGE_DIRECTORY)

    if not images:
        print(
            f"No supported images were found in:\n"
            f"{IMAGE_DIRECTORY}"
        )
        return

    print(f"Found {len(images)} image(s).")
    print(f"Model: {VISION_MODEL}")
    print(f"Ollama: {OLLAMA_BASE_URL}")
    print(f"Output: {OUTPUT_FILE}")
    print()

    output_sections = [
        "# Refined image descriptions",
        "",
        f"Model: `{VISION_MODEL}`",
        "",
        "Pipeline: two-pass visual analysis",
        "",
        (
            "The first pass creates a draft. The second pass reviews the "
            "draft while examining the original image again."
        ),
        "",
        "---",
        "",
    ]

    # Write the header immediately so the output file exists even if the
    # program is interrupted later.
    write_output(output_sections)

    for index, image_path in enumerate(images, start=1):
        print(
            f"[{index}/{len(images)}] "
            f"Processing {image_path.name}"
        )

        try:
            print("  Encoding image ...")
            encoded_image = encode_image(image_path)

            print("  Pass 1/2: initial visual analysis ...")
            first_started = time.perf_counter()

            draft_result = analyze_image(encoded_image)

            first_wall_time = time.perf_counter() - first_started

            print(
                f"  First pass completed in "
                f"{first_wall_time:.1f} seconds."
            )

            print("  Pass 2/2: visual review and correction ...")
            second_started = time.perf_counter()

            final_result = review_description(
                encoded_image=encoded_image,
                draft=draft_result.content,
            )

            second_wall_time = time.perf_counter() - second_started

            print(
                f"  Second pass completed in "
                f"{second_wall_time:.1f} seconds."
            )

            output_sections.extend(
                build_success_section(
                    image_path=image_path,
                    draft_result=draft_result,
                    final_result=final_result,
                    first_wall_time=first_wall_time,
                    second_wall_time=second_wall_time,
                )
            )

            print(
                f"  Finished in "
                f"{first_wall_time + second_wall_time:.1f} seconds."
            )

        except (OllamaError, OSError) as exc:
            print(f"  Error: {exc}")

            output_sections.extend(
                build_error_section(
                    image_path=image_path,
                    error=exc,
                )
            )

        # Save after every image so completed results are not lost if the
        # script is stopped later.
        write_output(output_sections)
        print()

    print("Processing complete.")
    print(f"Results written to:\n{OUTPUT_FILE}")


if __name__ == "__main__":
    main()