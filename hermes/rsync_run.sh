#!/bin/bash

# Original rsync command
# rsync -avzL --files-from=download_rsync.txt rsync://dtn.sdss.org/dr17/ dr17/

RSYNC_FILE="../download_rsync.txt"

# Define the destination folder
DESTINATION_FOLDER=""


# Number of parallel transfers
PARALLEL_TRANSFERS=1

# Move files in parallel
cat $RSYNC_FILE | xargs -P $PARALLEL_TRANSFERS -I {} rsync -avzL --partial rsync://dtn.sdss.org/dr17/{} dr17/{}


cat download_rsync.txt | xargs -P 1 -I {} rsync -avzL --partial rsync://dtn.sdss.org/dr17/{} dr17/{}

Hi Joel,

Thank you so much for your quick and helpful response! I really appreciate the guidance you’ve provided.

I’ll definitely use the specObj.class_noqso = "GALAXY" in my SQL query to get a more accurate classification. That makes a lot of sense, and will save me a lot of time in my experiments.

The rsync feature sounds eciting and it is clearly better than the scripts I've working with. I appreciate you laying out the steps for me.

Your support means a lot, and I’m grateful for all the insights you’ve shared. Thanks again for taking the time to help me out!

Best,
Raahul Singh
