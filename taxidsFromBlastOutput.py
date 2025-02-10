#!/usr/bin/python3
# -*- coding: utf-8 -*-

import polars as pl
import csv
import argparse
import time     

FILTER = (
    (pl.col("evalue") < 1e-4) & 
    (pl.col("qcovhsp") > 75) & 
    (pl.col("scovhsp") > 75)
)

def main(input_file, output_file):
    
    # Assume that mapping files are in the same directory as the script
    # If not ask for script " createTaxonMap.sh " 
    with open("eukaryotes.csv") as f:
        reader = csv.reader(f, delimiter=";")
        eukaryotes = {int(row[0]): int(row[1]) for row in reader if row[0] != "" and row[1] != ""}

    with open("strain2species.csv") as f:
        reader = csv.reader(f, delimiter=";")
        species_map = {int(row[0]): int(row[1]) for row in reader if row[0] != "" and row[1] != ""}

    start = time.perf_counter()

    request = (
        pl.scan_csv(input_file,
                    separator="\t",
                    has_header=True
        )
        .filter(
            FILTER
        )
        .with_columns(pl.col("staxids").str.split(";"))
        .explode("staxids")
        .with_columns(pl.col("staxids").cast(pl.Int32))
        .with_columns([
            #pl.col("staxids").replace(eukaryotes, default=0).alias("euk"),
            pl.col("staxids").replace_strict(species_map, default=-1, return_dtype=pl.Int32).alias("species")
        ])
        # .filter(pl.col("euk") == 1)
        .filter(pl.col("species") != -1)
        .group_by("qseqid")
        .agg(
            pl.col("species")
        )
        .with_columns(
            species = pl.col("species").cast(pl.List(str)).list.unique()
        )
        .select(["qseqid","species"])
    )

    grouped = request.collect()
    end = time.perf_counter()

    print(f"Elapsed time for data curration : {end-start}")

    grouped_dict = dict(zip(grouped["qseqid"].to_list(), grouped["species"].to_list()))

    print(f"Found {len(grouped_dict)} query proteins in the blast output")

    with open(f"{output_file}","w") as f: 
        for k,v in grouped_dict.items():

            v = " ".join(v)
            f.write(f"{k};{v}\n")

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Attribute to each query the species taxid of the subject.")
    parser.add_argument("--input", type=str, help="Input TSV file path")
    parser.add_argument("--output", type=str, help="Output file path")
    args = parser.parse_args()

    main(args.input, args.output)
