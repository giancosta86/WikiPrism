# WikiPrism

_Parse wiki pages and create dictionaries, fast, with Python_

## Introduction

**WikiPrism** is a Python library designed to:

1. Parse a **wiki XML file** in order to extract its **pages** - contained within \<page\> tags

1. Parse each page so as to extract **terms** from it - that will be added to a custom **Dictionary** instance

The above tasks can be easily combined in a sophisticated but user-friendly automated process called **extraction pipeline** - configurable via a dedicated descriptor object.

WikiPrism focuses on **speed** - a goal achieved by introducing careful design choices, including the _transparent_ use of **parallelism** via both _multithreading_ and _multiprocessing_, while orchestrating several elements provided by the Eos library and without losing architectural _simplicity_.

In this guide, we are going to see an overview of the library.

## Installation

To install **WikiPrism**, just run:

> pip install info.gianlucacosta.wikiprism

or, if you are using Poetry:

> poetry add info.gianlucacosta.wikiprism

Then, you'll be able to access the **info.gianlucacosta.wikiprism** package and its subpackages.

## Extracting pages from a wiki source

To perform _blazing-fast_ parsing, WikiPrism relies on customized **SAX**; more precisely, it expects a wiki file having whatever structure one prefers, as long as its pages comply with the following schema:

```xml
<page>
  ...
  <title>The page title</title>
  ...
  <text>The page content</text>
  ...
</page>
```

Autrement dit, as long as the XML file contains - anywhere - one or more **\<page\>** tags having the described structure, WikiPrism will be able to detect them.

### In practice

Parsing can be performed via Python's standard functions within the [xml.sax](https://docs.python.org/3.10/library/xml.sax.html) namespace - especially:

- **parse()** - to parse an XML file

- **parseString()** - to parse an XML string

Both functions require a **ContentHandler** and an optional **ErrorHandler** - and that's precisely why WikiPrism provides two dedicated classes:

- **WikiContentHandler**

- **WikiErrorHandler**

In particular, **WikiContentHandler**'s constructor expects:

- a _callback_, that receives a **Page** object - containing just the **title** and **text** attributes - as soon as a valid page is found by the SAX parser

- a _ContinuationProvider_, that is a () -> bool function periodically called: should it return False, the parsing would end by raising a **WikiSaxCanceledException**

For more details, please consult the docstrings.

## Creating dictionaries

Python has dictionaries - intended as hash maps - but WikiPrism introduces the abstract class named **Dictionary\[TTerm\]** as a generic container of _terms_, which can be language elements like nouns, verbs, conjunctions, or even anything else, according to one's linguistic model... the exact purpose of each dictionary is up to the developer.

**Dictionary\[TTerm\]** is a sort of generic repository for _terms_, via its 2 main abstract methods:

- **add_term()**: adds a term to the dictionary - actually writing to the storage technology and maybe preventing duplication

- **execute_command()**: runs a command string on the (arbitrary) underlying storage and returns a **Result\[DictionaryView\]** - that is, à la Rust, either a **DictionaryView** object (a table-like DTO with both headers and data rows) or an **Exception**

Dictionary also has other abstract methods to implement, but it never makes assumptions about its internal data storage; consequently, for convenience, WikiPrism also provides concrete subclasses:

- **InMemoryDictionary\[TTerm\]** - adding terms to a Python set, but unable to perform commands

- **SqliteDictionary\[TTerm\]** - dictionary backed by a SQLite db and passing commands to the related SQL interpreter

For further details, please consult the docstrings.

## The extraction pipeline

To combine wiki page extraction and dictionary creation into a performant, automated and easy-to-use process, WikiPrism defines _extraction pipelines_.

Running an extraction pipeline basically resolves to:

1. Creating a custom subclass of **PipelineStrategy\[TTerm\]** - or its SQLite-oriented subclass, **SqlitePipelineStrategy\[TTerm\]**

1. Invoking the **run_extraction_pipeline()** function, which only expects an instance of the strategy

**run_extraction_pipeline()** executes the pipeline _in a separate thread_ (plus its own subthreads and a process pool), therefore it returns a **PipelineHandle** - an object with the following methods:

- **join()** - to wait for its completion - and supporting the same parameters as Thread's [join()](https://docs.python.org/3.10/library/threading.html?highlight=thread#threading.Thread.join) method

- **request_cancel()** - to stop the pipeline in a clean way, as soon as possible

For more details, please consult the docstrings, the tests, and possibly the whole open source [Cervantes](https://github.com/giancosta86/Cervantes/) project.

## Related projects

- [Cervantes](https://github.com/giancosta86/Cervantes/) - WikiPrism applied to Wikcionario in order to explore **Spanish** morphology with Python

- [Eos-core](https://github.com/giancosta86/Eos-core) - type-checked and dependency-free modern utility library for Python
