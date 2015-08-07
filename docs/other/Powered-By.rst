Powered By Scalding
===================

Want to be added to this page? Send a tweet to `@scalding <https://twitter.com/scalding>`_ or open an issue.

================================= ================================================================  =====================================================================================================================
Company                           Scalding Use Case                                                 Code
================================= ================================================================  =====================================================================================================================
`Twitter`_                        We use Scalding often, for everything from custom ad targeting
                                  algorithms, market insight, click prediction, traffic quality to
                                  PageRank on the Twitter graph. We hope you will use it too!
`Etsy`_                           We're starting to use Scalding alongside the JRuby Cascading
                                  stack described `here`_.
                                  More to come as we use it further.
`Ebay`_                           We use Scalding in our Search organization for ad-hoc data
                                  analysis jobs as well as more mature data pipelines that feed
                                  our production systems.
`Snowplow Analytics`_             Our data validation & enrichment process for event analytics is   `GitHub <https://github.com/snowplow/snowplow/tree/master/3-enrich/hadoop-etl>`__
                                  built on top of Scalding.
`PredictionIO`_                   Machine-learning algorithms build on top of Scalding.             `GitHub <https://github.com/PredictionIO/PredictionIO/tree/master/process/engines>`__
`Gatling`_                        We've just rebuilt our reports generation module on top of
                                  Scalding. Handy API on top of an efficient engine.                `GitHub <https://github.com/gcoutant/gatling-scalding/tree/master/src/main/scala/com/excilys/ebi/gatling/scalding>`__
`SoundCloud`_                     We use Scalding in our search and recommendations production
                                  pipelines to pre and post-process data for various machine
                                  learning and graph-based learning algorithms. We also use
                                  Scalding for ad-hoc and regular jobs run over production logs
                                  for things like click tracking and quality evaluation on search
                                  results and recommendations.
`Sonar`_                          Our platform is built on Hadoop, Scalding, Cassandra and Storm.
                                  See Sonar's `job listings <http://www.sonar.me/jobs>`_.
`Sky`_                            Sky is using Scalding on Hadoop and utilizing HBase through the
                                  SpyGlass library for statistical analysis, content related jobs
                                  and reporting.
`LivePerson`_                     LivePerson's data science group is using Scalding on Hadoop, to
                                  develop machine learning algorithms and big data analysis.
`Sharethrough`_                   Sharethrough uses Scalding throughout our production data
                                  infrastructure. We use it for everything from advertiser
                                  reporting and ML feature engineering, to ad targeting and click
                                  forecasting.
`LinkedIn`_                       Scalding is being used at LinkedIn both at the Product Data
                                  Science team and the Email Experience team.
`Stripe`_                         Stripe uses Scalding for ETL and machine learning to support our
                                  analytics and fraud prevention teams.
`Move`_                           Move uses Scalding on Hadoop for advanced analytics and
                                  personalization for `Realtor.com <http://www.realtor.com/>`_ and
                                  its mobile real estate apps.
`Tapad`_                          Tapad uses scalding to manage productized analytics and
                                  reporting, internal ad-hoc data mining, and to support our
                                  data science team's research and development efforts.
`CrowdStrike`_                    CrowdStrike employs Scalding in our data science and data mining
                                  pipelines as part of our big data security platforms in
                                  research, development, product and customer endpoints. We have
                                  plans to open source our Scalding API (AWS, EMR) on github.
`Tumblr`_                         Tumblr uses scalding as a sort of MVC framework for Hadoop.
                                  Applications include recommendations/discovery, spam detection,
                                  and general ETL.
`Elance`_                         Elance uses scalding for constructing data sets for search
                                  ranking, recommendation systems, other modeling problems.
`Commonwealth Bank Of Australia`_ Commbank uses scalding as a key component within its big data     `GitHub <https://github.com/CommBank>`__
                                  infrastructure. Both on the ETL side, and for the implementation
                                  of data science pipelines for building various predictive
                                  models.
`Sabre Labs`_                     Sabre Labs uses Scalding for ETL and ad hoc data analysis of
                                  trip information.
`gutefrage.net`_                  gutefrage.net uses Scalding for its Data Products and general
                                  ETL flows.
`MediaMath`_                      MediaMath uses Scalding to power its Data Platform, the
                                  centralized data store that powers our ad hoc analytics,
                                  client log delivery and new optimization/insight-based products.
`The Search Party`_               The Search Party is using Scalding to build production machine
                                  learning libraries for clustering, recommendation and text
                                  analysis of recruitment related data. Scalding is a breath of
                                  fresh air!
`Opower`_                         Opower uses Scalding and
                                  `KijiExpress <https://github.com/kijiproject/kiji-express>`_ to
                                  analyze the world's energy data and extract machine
                                  learning-based insights that power behavior change.
`Barclays`_                       Barclays uses Scalding for Data Warehousing, ETL and data
                                  tranformation into columnar (query optimized) data formats.
`Devsisters`_                     Devsisters uses Scalding for game log analysis
                                  (https://github.com/twitter/scalding/issues/1264).
================================= ================================================================  =====================================================================================================================

.. _Twitter: http://twitter.com
.. _Etsy: http://etsy.com
.. _Ebay: http://www.ebay.com
.. _Snowplow Analytics: http://snowplowanalytics.com
.. _PredictionIO: http://prediction.io
.. _Gatling: http://gatling-tool.org
.. _Soundcloud: http://www.soundcloud.com
.. _Sonar: http://www.sonar.me
.. _Sky: http://www.sky.com
.. _Liveperson: http://www.liveperson.com
.. _Sharethrough: http://www.sharethrough.com/engineering
.. _LinkedIn: http://data.linkedin.com/team
.. _Stripe: http://stripe.com
.. _Move: http://www.move.com
.. _Tapad: http://www.tapad.com
.. _CrowdStrike: http://www.crowdstrike.com
.. _Tumblr: http://www.tumblr.com
.. _Elance: http://www.elance.com
.. _Commonwealth Bank of Australia: https://www.commbank.com.au
.. _Sabre Labs: http://sabrelabs.com
.. _gutefrage.net: http://www.gutefrage.net
.. _MediaMath: http://www.mediamath.com
.. _The Search Party: http://www.thesearchparty.com
.. _Opower: http://www.opower.com
.. _Barclays: http://www.barclays.co.uk
.. _Devsisters: http://www.devsisters.com


.. _here: http://codeascraft.etsy.com/2010/02/24/analyzing-etsys-data-with-hadoop-and-cascading/
