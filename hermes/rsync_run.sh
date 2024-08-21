#!/bin/bash

# Original rsync command
# rsync -avzL --files-from=download_rsync.txt rsync://dtn.sdss.org/dr17/ dr17/

RSYNC_FILE="download_rsync.txt"

# Define the destination folder
DESTINATION_FOLDER=""


# Number of parallel transfers
PARALLEL_TRANSFERS=1000

# Move files in parallel
cat $RSYNC_FILE | xargs -P $PARALLEL_TRANSFERS -I {} rsync -avzL --partial rsync://dtn.sdss.org/dr17/{} $DESTINATION_FOLDER/dr17/{}
