source activate mobility
python /home/ubuntu/smartmobpub/src/database/cron_script.py | awk '{ print strftime("%c: "), $0; fflush(); }' > /home/ubuntu/smartmobpub/output.log
