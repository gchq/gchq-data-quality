#!/bin/sh

# Change this to reflect which folders you want to include.
folders="./src ./tests"

# Change this to the file extension you want to apply the check for.
file_extension="*.py"

# Change this to the copyright notice you want to check for.
copyright_text="(c) Crown Copyright GCHQ"

# Leave this variable alone, it's used to count the number of files missing the notice.
missing=0

# Iterate over the indicated files in the folder
for i in  $(find $folders -name $file_extension); 
do
  # Look for the copyright text by grepping the current file
  if ! grep -q "$copyright_text" $i
  then
    echo "Missing copyright notice in $i"
    missing=$((missing + 1))
  fi
done

# If any files were missing the header - exit 1 ("error") - this will cause any build 
# pipelines to fail.
if [ "$missing" -gt 0 ]
then
  echo "\n-------------------------------------------------"
  echo "Add copyright headers to the above $missing files.\n"
  exit 1
fi