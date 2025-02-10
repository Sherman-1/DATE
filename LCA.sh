#!/usr/bin/bash

total=$(wc -l < taxids.txt)

# Install https://bioinf.shenwei.me/taxonkit/
# L'installation est un peu longue car elle installe en local 
# Le fichier de taxonomy du NCBI ( pas très lourd ne t'en fais pas )
# Si tu continues dans une branche de la bioinfo qui utilise de l'information phylogénétique : 
# taxonkit est un des outils les + rapides que j'ai trouvé 

cat taxids.txt | tqdm --total=$total --unit lines --unit_scale --desc "Processing TaxIDs" | while IFS=";" read -r identifier taxids; do
    if [[ -n "$taxids" ]]; then  
        result=$(echo "$taxids" | taxonkit lca -b 2G --skip-deleted --skip-unfound --threads 4 2> /dev/null)  
        last_field=$(echo "$result" | awk '{print $NF}')  
        echo "$identifier;$last_field" >> lcas.txt  
    fi
done

sort -u lcas.txt > tmp 
cat tmp > lcas.txt && rm tmp