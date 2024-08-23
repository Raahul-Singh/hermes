#!/bin/bash

# Define your Google Drive remote
REMOTE="gdrive:hermes/75k"

# Define the destination folder
DESTINATION_FOLDER="gdrive:hermes/first75k/r/"

# File containing the list of filenames
FILELIST="drive_paths/temp.txt"

# Number of parallel transfers
PARALLEL_TRANSFERS=1000

# Move files in parallel
cat "$FILELIST" | xargs -I{} -P $PARALLEL_TRANSFERS rclone move "$REMOTE/{}" "$DESTINATION_FOLDER" --transfers 1000 --checkers 1000 --progress
