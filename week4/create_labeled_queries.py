import os
import argparse
import xml.etree.ElementTree as ET
import pandas as pd
import numpy as np
import csv
import re

# Useful if you want to perform stemming.
import nltk

stemmer = nltk.stem.PorterStemmer()


def transform_query(query):
    return " ".join((stemmer.stem(w) for w in re.sub("[^0-9a-zA-Z]+", " ", query).lower().split()))


categories_file_name = r'/home/fboutin/datasets/bbuy/product_data/categories/categories_0001_abcat0010000_to_pcmcat99300050000.xml'

queries_file_name = r'/home/fboutin/datasets/bbuy/train.csv'
# queries_file_name = r'small_train.csv'
output_file_name = r'/home/fboutin/datasets/bbuy/labeled_query_data.txt'

parser = argparse.ArgumentParser(description='Process arguments.')
general = parser.add_argument_group("general")
general.add_argument("--min_queries", default=1, help="The minimum number of queries per category label (default is 1)")
general.add_argument("--output", default=output_file_name, help="the file to output to")

args = parser.parse_args()
output_file_name = args.output

if args.min_queries:
    min_queries = int(args.min_queries)

# The root category, named Best Buy with id cat00000, doesn't have a parent.
root_category_id = 'cat00000'

tree = ET.parse(categories_file_name)
root = tree.getroot()

# Parse the category XML file to map each category id to its parent category id in a dataframe.
print("Parsing categories...")
categories = []
parents = []
depths = []
for child in root:
    id = child.find('id').text
    cat_path = child.find('path')
    cat_path_ids = [cat.find('id').text for cat in cat_path]
    leaf_id = cat_path_ids[-1]
    if leaf_id != root_category_id:
        categories.append(leaf_id)
        parents.append(cat_path_ids[-2])
        depths.append(len(cat_path_ids))
parents_df = pd.DataFrame(list(zip(categories, parents, depths)), columns=['category', 'parent', 'depth'])
parents_df = parents_df.set_index('category')
print(f"Parsed {len(parents_df)} categories.")

# Read the training data into pandas, only keeping queries with non-root categories in our category tree.
print("Load queries...")
df = pd.read_csv(queries_file_name)[['category', 'query']]
num_queries = len(df)
df = df[df['category'].isin(categories)]
print(f'Loaded {len(df)} queries. {num_queries} before filtering.')

# Normalize queries
print("Normalize queries...")
df['processed_query'] = df['query'].map(transform_query)


def get_depth(category):
    return parents_df.at[category, 'depth']


print("Assign depths...")
df['category_depth'] = df['category'].map(get_depth)

# Roll up categories to ancestors to satisfy the minimum number of queries per category.
print(f"Start rolling up {len(df['category'].value_counts())} categories ...")
for depth in reversed(range(df['category_depth'].max() + 1)):
    print(f"Rolling up categories of depth {depth}...")
    for category in list(
            df[df['category_depth'] == depth]['category'].value_counts()[lambda x: x < min_queries].keys()):
        # print(f'Rolling up {category}')
        target_category = parents_df.at[category, 'parent']
        assert target_category != category
        print(f'Rolling up {category} to {target_category}...')
        df.loc[df['category'] == category, 'category_depth'] = depth - 1
        df.loc[df['category'] == category, 'category'] = target_category
print(f"Finished rolling up categories : {len(df['category'].value_counts())} categories remaining with at least {min_queries} queries.")

# Create labels in fastText format.
df['label'] = '__label__' + df['category']

# Output labeled query data as a space-separated file, making sure that every category is in the taxonomy.
df = df[df['category'].isin(categories)]
df['output'] = df['label'] + ' ' + df['processed_query']
df[['output']].to_csv(output_file_name, header=False, sep='|', escapechar='\\', quoting=csv.QUOTE_NONE, index=False)
