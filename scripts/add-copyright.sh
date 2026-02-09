#!/bin/sh

# Change this to reflect which folders you want to include.
folders="./src ./tests"

# Change this to the file extension you want to apply the check for.
file_extension="*.py"

# Change this to the copyright notice you want to check for.
copyright_notice="(c) Crown Copyright GCHQ"

# Comment syntax
comment_start="#"
comment_end=""

# Iterate over the indicated files in the folder / extension
for i in $(find $folders -name $file_extension); 
do
  # If the file already contains the copyright notice, do nothing.
  if ! grep -q "$copyright_notice" $i
  then
    echo "Writing crown copyright header to $i"
    
    # Pipe the comment into a cat of the file, save to a new temporary file and then
    # move the temporary file over the top of the original file.
    echo "$comment_start $copyright_notice $comment_end\n" | cat - $i > $i.new && mv $i.new $i
  fi
done