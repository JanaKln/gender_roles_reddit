# Gender_roles_Reddit

This repository holds the code base for the paper: "Gender Roles in Parenting in Times of Crisis: An Examination with Reddit Data".
With the use of Reddit data, trends in topics discussed over the course of the COVID-19 pandemic are analysed with BERTopic. The two subreddits r/Mommit and r/daddit are used.
Furthermore, operationalizing gender roles with posts that are about home responsibilities, trends in gender roles over the course of the COVID-19 pandemic are analysed.

## Table of Contents

- [Installation](#installation)
- [Data access](#data-access)
- [Usage](#usage)
- [Support and Contact](#support-and-contact)


## Installation
Download the whole repository. Check this link to get all working files necessary for the Code: https://bwsyncandshare.kit.edu/s/9S5waJkxFnrfJLL.
The code will also work when you follow the steps in [Data access](#data-access).
The working files need to be located in the working directory of the code. In a virutal environment install all libaries from the requirements.txt file (pip install -r requirements.txt).

## Data access
The Reddit data come from the data dumps provided by pushift.io. However, they are not available anyomore from where I accessed the data for this paper: "https://files.pushshift.io/reddit/".
Instead, the data are now provided via academic torrents: "https://academictorrents.com/details/7c0645c94321311bb05bd879ddee4d0eba08aaee".
To download the data, you need a transmission torrent client, for example: "https://transmissionbt.com/". Choose the months you want to get the submissions and comments for and download the zst files.

## Usage
The Repository holds several python scripts and jupyter notebooks. In the following I will present the purpose of each and also when to execute which.
- data_dump_read.py
  - The .zst-files are filtered for the subreddits of interest. Data is written into text files.

- read_in_txt.py
  - the text files are further processed. Submissions and comments for all months of interst from r/Mommit and r/daddit, respectively, are combined to one csv file.

- preprocessing_BERTopic.py
  - data cleaning and preprocessing

- data_expl_viz.ipynb
  - visualizations and descriptive statistics for the Reddit data

- BERTopic_all_models.ipynb
  - all BERTopic models
  - without outlier reduction

- BERTopic_evaluation_analysis.ipynb
  - evaluation of BERTopic models
  - with outlier reduction and without

- topics_traditional_roles.ipynb
  - analysis of posts with topics on home responsibilities

- posts_traditional_roles.ipynb
  - analysis of posts on home responsibilities

- visualization_fig4and7.ipynb
  - seperate notebook for combined visualization of the two approaches of identifiying posts on home responsibilities


## Support and Contact

- author: Jana Klein
- e-mail: klein.janaklein@web.de
