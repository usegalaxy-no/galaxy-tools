# Introduction
This tool extracts statistics data from a Galaxy installation in a format compatible to [Telegraf of TICK stack](https://www.influxdata.com/time-series-platform/)

# Setup
```bash
git clone https://github.com/elixir-no-nels/galaxy-admin.git
cd galaxy-admin
virtualenv -p python3 .venv
source .venv/bin/activate
pip install -r requirements

# update config by editing the database url -> hint: look in config/galaxy.ini or config/galaxy.yml
vim galaxy.json
```

# Testing 
```bash
<INSTALL_DIR>/.venv/bin/python <INSTALL_DIR>/bin/galaxy_stats.py -c <INSTALL_DIR>/<CONFIG-FILE> stats

```

# Telegraf configuration
```bash
#1. run the script generation command
<INSTALL_DIR>/.venv/bin/python <INSTALL_DIR>/bin/galaxy_stats.py -c <INSTALL_DIR>/<CONFIG-FILE> tick-config

#2. copy the output into your telegraf config file close to the other input plugins -> hint: default file is /etc/telegraf/telegraf.conf

#3. test your telegraf 
telegraf --test  --config /etc/telegraf/telegraf.conf

#4. if all is fine, restart telegraf
service telegraf restart 
```