# Davinci Codebase

![DaVinci logo](./docs/images/logo.png)

## Repository

This repo contains Python package of frequently needed functions/classes for Davinci projects. [See the documentation here for details](http://172.19.14.118:1117/index.html).

## Installation

To install the Python package to a local or production machine,
use the following from the parent working directory of the repo:

```
python -m pip install -e ./codebase
```

You should now have access to the library 'davinci' within
your Python scripts.

## Docker Usage

![Docker](./docs/images/DockerProgress.drawio.svg)


## Monitoring Dashboard
The dev tools in davinci include a dashboard to monitor DS/ML
activity. To run this, make sure Dash (plotly) is installed. Then
do:

```
cd davinci/dev_tools/monitor_dash
python app.py
```

This will start a local server with the dashboard. Look at the terminal
output for the URL to view.

## Documentation

To view or update documentation for 'davinci', follow the below.
Note that you must install 'davinci' prior to this:

```
cd codebase/docs
./make html
```

Then open the file docs/build/index.html, and the interactive documentation will open in a browser.