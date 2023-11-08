# LinkedIn Automation Script

This Python script automates interactions on LinkedIn, such as downloading data archives, for multiple LinkedIn accounts provided in a CSV file. You can also specify a custom download directory for saving the downloaded files.

## Prerequisites

- Python 3.x
- Required Python packages (install using `pip`):

## Command
```bash
python3.x -m pip install requirements.txt
```
## Setup

1. Clone this repository to your local machine.

2. Create a CSV file named `credentials.csv` with two columns: `email` and `password`, containing the LinkedIn account credentials you want to use for automation. If you want to specify a different CSV file, you can do so using the `--csv_file` command-line argument (see "Usage" below).

3. Setup download path if needed in config.py by default it's /data . If you want to specify a different download path, you can do so using the `--download_path` command-line argument (see "Usage" below).

4. Download the ChromeDriver executable suitable for your system and place it in the project folder. Make sure it matches your Google Chrome version.

5. Modify the `config.py` file to specify the XPath configurations and any other necessary settings for LinkedIn automation.

## Usage

To run the script, open a terminal and navigate to the project directory. Use the following command:

```bash
python main.py --csv_file=/path/to/your/csv/file.csv --download_path=/path/to/your/download/directory
```
Or this command if you have set path's in config.py

```bash
python main.py
```
