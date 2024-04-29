## Elastic Bulk Uploader

Tool for creating and bulk uploading data to Elasticsearch. 

### Prereqs

- Install Python 3.12 (ideally using venv)
- Install Poetry

### Test Data Generator

The following command generates 10 test files in the `./docs` folder

```bash
python test_data_generator.py -o ./docs -n 100
```

### Bulk Data Importer

The following command creates an index named `search-load-test` using the index config in `./configs/index-config.json`

```bash
python elastic.py --action create_index --index-name search-load-test --index-config configs/index-config.json 
```

The following command uploads documents with 4 threads in chunks of 100 documents per thread:

```bash
python elastic.py --action bulk_upload --index-name search-load-test --input-folder docs/ --pipeline-name elser-v6-test --chunk-size 5 --thread-count 1
```
