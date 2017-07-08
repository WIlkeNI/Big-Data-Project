if [ -z "$1" ]; then
    echo "No argument supplied."
    exit
fi

for item in $(ls -p ./stream | grep -v /); 
	do cat stream/$item; 
	sleep ${1}; 
done; 
