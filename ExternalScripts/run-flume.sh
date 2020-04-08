# sample flume runner - takes a 10 line sample of the sensor log
#$1 takes in the file name of the data in zip (assumes csv content)
#to run : sh run-flume.sh data.zip
#folder structure:
#root
# - data // data.zip is located here
#  - sensor

cd data
unzip -o $1.zip
head -10 Complete_$1.csv > sensor/sample.csv

cd sensor
sed -i '1d' sample.csv
cd ../..
flume-ng agent -n "a1" -f /home/cloudera/ca/config/flume-sensor-taker.conf -Dflume.root.logger=DEBUG,console
