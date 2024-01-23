### Original Goal
This repository is seperate from the "Steak Soup Salad" project, which would have been an extension on the data collected from this. It would involve using ChromaDB to index the documents outputted from this project, then use LangChain to connect the Llama 2 LLM to provide a "low-shot food classifier" pipeline.

---
### Change-of-scope
Originally, I had intended for this project to be the base for a pre-training then fine-tuning dataset for a RoBERTa-like Large Language Model (LLM) variant. This was not feasible due to the compute cost of training RoBERTa from scratch. Fine-tuning was also not an option for the same reasons.

Before moving on, I had done research on BERT, BART, RoBERTa, ELECTRA, and other LLMs like Llama 2, and GPT-4. Most of these models are bi-directional (BERT, BART, RoBERTa, ELECTRA) while some are generative (Llama 2, GPT-4). The only major difference between the two is archetecture, and ability to understand instrinsic meanings of words (best "understood" by bi-directional models), and quality of generated responses (Llama 2, GPT-4).

I had also done research on BERTopic and topic modelling as a task for LLMs. Topic modelling was best for set data to cluster and generate topics related to each cluster. This was not ideal since this limits the possibility of cluster labels.

I later found extreme-multilabel classification, zero-shot generative classification, and other similar projects. This was even less feasible as it requires magnitudes of compute power. Some models I researched were GROOV, ECLARE and DECAF.

After researching this many models, I shifted my main goal instead to get large amounts of data for these models to consume. Since processing and fetching this kind of data was less compute-heavy, it was a good goal to set before tackling anything else.

I additionally set the goal of setting up and trying out PineconeDB and ChromaDB with smoe sample vectors. PineconeDB was more limited in it's abilities, and ChromaDB was time-limited as I used the 30-day free-trial period. A final goal that I had set was to set up some sort of vector-based database (ChromaDB) with LangChain and Llama 2 to have my final project. I was not able to achieve this, but had comlpeted everything (aside from training my own model) up until this point.