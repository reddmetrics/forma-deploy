emails="rkraft4@gmail.com dan.s.hammer@gmail.com"
echo "Sending update emails to $emails"

echo
echo "Updating fires"
python ./fires_update.py --bucket modisfiles --staging-bucket formastaging -e $emails

echo
echo "Updating monthly 1000m MODIS data"
python ./modis_update.py -r 1000 -i 32 --bucket modisfiles --staging-bucket formastaging -e $emails

echo
echo "Updating 16-day 1000m MODIS data"
python ./modis_update.py -r 1000 -i 16 --bucket modisfiles --staging-bucket formastaging -e $emails

echo
echo "Updating 16-day 500m MODIS data"
python ./modis_update.py -r 500 -i 16 --bucket modisfiles --staging-bucket formastaging -e $emails

echo
echo "Updating 16-day 250m MODIS data"
python ./modis_update.py -r 250 -i 16 --bucket modisfiles --staging-bucket formastaging -e $emails

echo
echo "Updates complete"
