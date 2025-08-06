# MSH MTConnect Data Tools

This repository contains tools for recording, analyzing, and visualizing MTConnect data streams from CNC machines at Mekanisk Service Halden (MSH). The suite includes a data recorder, sampling rate analyzer, sequence integrity checker, and an interactive playback simulator.

## 📁 Project Structure

### 1. standalone_recorder.py
Purpose:  
Polls MTConnect data from configured machine endpoints and stores it as newline-delimited JSON (.jsonl) files, one per day.

Features:
- 5Hz polling rate.
- Stores all values, including "UNAVAILABLE".
- Handles multiple machines (defined in SOURCES).

Usage:
python standalone_recorder.py

### 2. sampling_rate_analysis.py
Purpose:  
Analyzes the actual sampling frequency of the recorded data per day and visualizes average sampling rate vs. expected 5Hz.

Outputs:
- CSV summary: sampling_rate_summary.csv
- Plot: daily_sampling_rate.png

Usage:
python sampling_rate_analysis.py

### 3. analyze_missing_sequence_number.py
Purpose:  
Checks for gaps in the MTConnect sequence numbers to detect data loss. Reports how many sequence values are missing per day and produces a bar chart.

Outputs:
- Console summary of missing sequences.
- Plot: missing_sequences_per_day.png

Usage:
python analyze_missing_sequence_number.py

### 4. data_simulator.py
Purpose:  
Visualizes .jsonl data interactively with playback controls using Streamlit. Lets you simulate the data flow and examine variable trajectories over time.

Features:
- Play/pause button
- Adjustable playback speed
- Graphs selected variables over time
- Highlights current value with timestamp

Usage:
streamlit run data_simulator.py

### 5. auto_connect.py
Purpose:  
(Utility or support script — please update this section if needed based on final content.)

## 🔧 Requirements

Install dependencies via pip:
pip install -r requirements.txt

Typical dependencies:
- streamlit
- pandas
- matplotlib
- requests

## 📂 Data Format

All data is stored as .jsonl files under a data/ directory, with each line representing one observation and containing a timestamp and machine name, alongside telemetry fields (e.g., SpindleSpeed, ToolNumber, Position, etc.).

Example entry:
{
  "sequence": 245,
  "timestamp": "2025-06-03T12:01:02.345678",
  "machine": "VTC",
  "SpindleSpeed": 3200,
  "ToolNumber": 22
}
