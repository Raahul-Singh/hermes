# Save the current directory
ORIGINAL_DIR=$(pwd)
FITS_DIR="/Users/raahulsingh/work/hermes/hermes/fits"

# Change to the FITS_DIR
cd "$FITS_DIR" || exit

# Process the .bz2 files in parallel
find . -name "*.bz2" -print0 | xargs -0 -n 1 -P 4 bash -c '
    for file; do
        bzip2 -d "$file"
    done
' _

# Return to the original directory
cd "$ORIGINAL_DIR" || exit

#  needs work to remove corrupted files that cannot be unzipped
