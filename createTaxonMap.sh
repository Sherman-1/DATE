#!/usr/bin/bash 

taxonkit list --ids 2759 --indent "" | awk '$1 != "" {print $1";1"}' > eukaryotes.csv

taxonkit list --ids 1 --indent "" | taxonkit filter -L species --discard-noranks \
    | taxonkit reformat -I 1 --format "{s}" --show-lineage-taxids \
    | cut -f 1,3 | awk '{print $1";"$2}' > strain2species.csv

# Don't forget that species are mapping to themselves !
taxonkit list --ids 1 --indent "" | taxonkit filter -E species \
    | awk '{print $1";"$1}' >> strain2species.csv