mkdir data
cd data

# get file list
files=`curl -s "http://s3.amazonaws.com/bzwbk-hackathon/" 2>&1 | xpath "//Key" 2>/dev/null | perl -pe 's|<Key>(.*?)<\/Key>|\1\n|g' | grep hackathon.*TXT`

for file in $files; do
  if [ ! -f $file ]; then
  	echo $file
  	mkdir -p `echo $file | rev | cut -d"/" -f2- | rev`
  	wget -O $file http://s3.amazonaws.com/bzwbk-hackathon/$file
  fi
done

cd hackathon

hdfs dfs -mkdir -p /etl/manual/bzwbk
hdfs dfs -put . /etl/manual/bzwbk