<img src="https://softwareag-docs.s3.eu-west-1.amazonaws.com/documents/Software_AG_logo.svg" alt="Software AG" width="300"/>

Prior to executing the Docker Pull Command, downloading, using or installing the accompanying software product, please ensure to read and accept the terms applying to this offering:

[LIMITED USE LICENSE AGREEMENT FOR SOFTWARE AG DOCKER IMAGES](#saglicense)

# Terracotta

Terracotta is a comprehensive, distributed in-memory data management solution which caters to caching and operational storage use cases, and enables transactional and analytical processing.

Terracotta supports the following sub-systems:

1. A data storage sub-system called TCStore, that caters to operational database and compute functionality (Enterprise version).

2. A caching sub-system called Ehcache, that caters to caching functionality.

Both sub-systems are backed by the Terracotta Server, which provides a common platform for distributed in-memory data storage with scale-out, scale-up and high availability features.

### Usage limitations

Free trial is time-limited and for non-production use.  Trial license is available on https://www.terracotta.org/retriever.php?n=Terracotta101linux.xml or (http://www.terracotta.org/downloads/) and is limited to 90 days and 50 GB of in-memory data.

Product requires license key obtained from Software AG to run.

If you wish to purchase a commercial license for Terracotta, please contact us here https://www.softwareag.com/corporate/contact/default.html

## List of all images available

    terracotta/ehcache-terracotta-server
    terracotta/ehcache-terracotta-config-tool
    terracotta/ehcache-terracotta-voter

## How to use these images

The Terracotta 10.x OSS offering includes the following:

* Ehcache 3.x compatibility
* Distributed In-Memory Data Management with fault-tolerance via Terracotta Server (1 stripe)
* In memory off-heap storage - take advantage of all the RAM in your server

> This image contains the Ehcache Server

### With default settings

Without any argument provided, the server will start in configuration mode with
some default settings set:

- 2 offheap resources
- config, etc located at `/terracotta/run`

```bash
docker run -d \
  -p 9410:9410 \
  -h ehcache-terracotta-server \
  --network terracotta-net \
  --name ehcache-terracotta-server \
  ehcache-terracotta-server:@version@
```

It is possible to change the offheap.

Also, the node can directly start activated with `DEFAULT_ACTIVATE`,
and the cluster name can be changed with `DEFAULT_CLUSTER_NAME`.

`DEFAULT_FAILOVER` can control if you want `availability`, `consistency` or `consistency:<n>`

```bash
docker run -d \
  -e DEFAULT_OFFHEAP="offheap-1:512MB,offheap-2:512MB,offheap-3:512MB" \
  -e DEFAULT_CLUSTER_NAME="tc-cluster" \
  -e DEFAULT_ACTIVATE="true" \
  -e DEFAULT_FAILOVER="availability" \
  -p 9410:9410 \
  -h ehcache-terracotta-server \
  --network terracotta-net \
  --name ehcache-terracotta-server \
  ehcache-terracotta-server:@version@
```

### Custom startup

It is possible to start a node by specifying the same arguments as the `start-tc-server.sh` script.

```bash
docker run -d \
  -p 9410:9410 \
  -h ehcache-terracotta-server \
  --network terracotta-net \
  --name ehcache-terracotta-server \
  ehcache-terracotta-server:@version@ \
  <arguments>
```

In that case, all the `DEFAULT_*` Docker environment variable are not considered, and you
must pass all the required arguments to the CLI to correctly start the server.

We strongly advise to place your data inside `/terracotta/run`

In case you would like to provide a configuration file, this can be done by mounting a read-only config
volume to `/terracotta/config`

Here is an example of a command line to start a node pre-activated from the CLI:

```bash
docker run -d \
  -p 9410:9410 \
  -h ehcache-terracotta-server \
  --network terracotta-net \
  --name ehcache-terracotta-server \
  ehcache-terracotta-server:@version@ \
  -config-dir "/terracotta/run/config" \
  -failover-priority availability \
  -offheap-resources "offheap-1:512MB,offheap-2:512MB" \
  -name "ehcache-terracotta-server" \
  -hostname "ehcache-terracotta-server" \
  -log-dir "/terracotta/run/logs" \
  -cluster-name "tc-cluster" \
  -auto-activate
```

### Adding persistence across container restart

Adding persistence is done by specifying a read-write volume in the Docker command-line:

```bash
-v /path/to/run-directory:/terracotta/run:rw
```

The default configuration outputs all data into only one folder: `/terracotta/run`.

If you are using the custom startup mode with your own arguments, **we strongly encourage to align your config to use the same paths.
We do not support any configuration writing at a location other than inside `/terracotta/run`**

Our containers are made in a way that `/terracotta/run` can be used to write data into. We do not support any other location.

Also, make sure the folder in the host machine has the appropriate chmod so
that the container uid (which is another uid for the host machine) will be
able to write into the mounted folder.

**Example:**

The command below will start a node in ACTIVE state, and will persist everything in the
host machine folder `/path/to/run-directory`.

```bash
docker run -d \
  -v /path/to/run-directory:/terracotta/run:rw \
  -v /path/to/config-directory:/terracotta/config:ro \
  -e DEFAULT_ACTIVATE="true" \
  -p 9410:9410 \
  -h ehcache-terracotta-server \
  --network terracotta-net \
  --name ehcache-terracotta-server \
  ehcache-terracotta-server:@version@
```

**WARNING:**

By default, the data is persisted into `/terracotta/run`.
The persisted data will be owned by the user id from the container so you might be unable to clean up the files from the
host machine.
Also makes sure that the chmod (777) is set to your mount folder so that the container user can write into the location.

Here is a cleanup procedure:

```bash
# Ensure  container is removed or stopped first
docker rm -f ehcache-terracotta-server
docker run --rm \
  -v /path/to/run-directory:/terracotta/run:rw \
  --entrypoint="" \
  ehcache-terracotta-server:@version@ \
  rm -f -R /terracotta/run/
```

### Considerations

- The `config` volume is **optional** and can be mounted **read-only** with the appropriate chmod
- The `run` volume is **optional** and must be mounted **read-write** with the appropriate chmod if persistence is required across container restart

## Addendum

### Additional Documentation

The full documentation is available from the Terracotta website: http://terracotta.org/documentation/

On the community website you can also find forum discussions and blog posts about using Terracotta: http://terracotta.org/community

### Feedback

For customers with a commercial license support is available via Empower as normal. For community users use the forums on http://terracotta.org/community.

### Trial license restrictions

Free trial is time-limited and for non-production use.  Trial license is available on https://www.terracotta.org/retriever.php?n=TerracottaDB101linux.xml (http://www.terracotta.org/downloads/) and is limited to 90 days and 50 GB of in-memory data.

Product requires license key obtained from Software AG to run.

If you wish to purchase a commercial license for Terracotta, please contact us here https://www.softwareag.com/corporate/contact/default.html

### Base Image

This product references the official [azul/zulu-openjdk-alpine](https://hub.docker.com/r/azul/zulu-openjdk-alpine) image as its base image. Software AG is not responsible for the contents of this base image.

<a name="saglicense"></a>
### THE LICENSE

With this Agreement, Software AG grants you - free of charge - a non-exclusive, non-transferable license to use and copy the Product and accompanying documentation on the number of computers, workstations or on terminals within a network as specified in the respective Product documentation (please refer to the respective section in the Release Notes relating to use restrictions) for your internal production use and for a time period defined below (see section License Validity). You must use the Product solely as described in its accompanying documentation. In no event may the Product be used to develop an integrated solution that requires for the Product to be integrated into your or any third party intellectual property in order to create a combined product that is provided to third parties. You may not pass on or distribute copies of the Product to any third party. You have the right to make one copy of the Product solely for archival and backup purposes. You may not decompile, disassemble, modify, decrypt, extract or otherwise reverse engineer, or make further copies of the Product or parts thereof. This Agreement is proof of your entering into this Agreement and you must retain it. This Agreement does not grant you the right to sublicense, transfer, rent, assign or lease the Product, in whole or in part.

### THIRD PARTY RESTRICTIONS

The Software may contain or include software applications for which the Software AG itself had to acquire a license to use from a third party ("Third Party Applications"). These Third Party Applications may be subject to additional license terms ("Third Party Terms"), which are identified below or made available under the web address http://softwareag.com/licenses.
The third party shall be entitled - only in relation to the respective third-party software - to exercise the rights of Software AG under this Agreement as a third party beneficiary directly against the licensee. Your use of the Third Party Applications will demonstrate your agreement to be bound by the Third Party Terms.
Your use of Java SE Platform Products is expressly subject to the terms and conditions set forth here: http://www.oracle.com/technetwork/java/javase/terms/license/index.html. You may not use or distribute these third party applications or its APIs on a stand-alone basis without the Product nor attempt to alter or modify it.
Software AG’s Product may reference dependencies on other Third Party Applications (e.g. database or operating system base layers) which are not part of the Product shipment and packaging and which are not linked to Software AG’s product in any way but which may be downloaded on execution of the Product package by Licensee. These Third Party Applications come with their own license terms and Software AG does not take liability of any kind for such dependencies.
SOFTWARE AG MAKES NO WARRANTIES OF ANY KIND, WHETHER EXPRESS OR IMPLIED, WITH REGARD TO ANY THIRD PARTY APPLICATIONS. ALL THIRD PARTY APPLICATIONS ARE PROVIDED "AS-IS," WITHOUT WARRANTIES OF ANY KIND. IN NO EVENT WILL SOFTWARE AG BE LIABLE TO YOU OR ANY THIRD PARTY FOR ANY DIRECT, INDIRECT, PUNITIVE, EXEMPLARY, INCIDENTAL, SPECIAL, OR CONSEQUENTIAL DAMAGES ARISING OUT OF THE THIRD PARTY APPLICATIONS, EVEN IF SOFTWARE AG HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGES OR LOSSES.
Your sole remedies with regard to the Third Party Applications will be as set forth in the relevant Third Party Terms, if any.

### LICENSE VALIDITY

This Agreement grants you a license for an indefinite period of time, subject to termination, as provided in this Agreement. The license will however limit your use of the Product to certain features, platforms or restrictions in capacity or other limitations incorporated by default, or - if applicable - by definition in the respective Product documentation. You accept these limitations and will in no event bypass these, whether by reverse engineering or other means.

### INTELLECTUAL PROPERTY

Except with respect to the Third Party Applications, Software AG or its affiliates and licensors are the sole owners of the intellectual property rights or industrial rights in and to the Product and accompanying user documentation or have the respective distribution rights. References made in or on the Product to the copyright or to other intellectual property or industrial property rights must not be altered, deleted or obliterated in any manner.
Except for the limited license granted in this Agreement, Software AG, its affiliates, and licensors reserve all other right, title, and interest in the Product. The name Software AG and all Software AG product names are either trademarks registered trademarks of Software AG and/or Software AG USA Inc. and/or its subsidiaries and/or its affiliates and/or their licensors. Other company and product names mentioned herein may be trademarks of their respective owners. No right, title or interest in any trademark or trade names of Software AG or its subsidiaries or its licensors is granted hereunder.
(c) Copyright 2022 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA and/or its subsidiaries and/or its affiliates and/or
their licensors.
You may provide suggestions, comments or other feedback (collectively, “Feedback”) to Software AG with respect to the Product. Feedback is entirely voluntary and Software AG is not required to hold it in confidence. Software AG may use Feedback for any purpose without obligation of any kind. To the extent a license is required under your intellectual property rights to make use of the Feedback, you grant Software AG an irrevocable, non-exclusive, perpetual, royalty-free license to use the Feedback in connection with Software AG’s business, including the enhancement of the Products.

### CONFIDENTIALITY

The Product is confidential and proprietary information of Software AG and its licensors, and may not be disclosed to third parties. You shall use such information only for the purpose of exercising the Limited Use License Agreement to the Product and shall disclose confidential and proprietary information only to your employees who require such information for the purpose stated above. You agree to take adequate steps to protect the Product from unauthorized disclosure or use.

### LIMITED WARRANTY

The Product is provided "as is" without any warranty whatsoever.
TO THE MAXIMUM EXTENT PERMITTED BY LAW, SOFTWARE AG AND ITS AFFILIATES AND LICENSORS DISCLAIM ALL WARRANTIES WITH RESPECT TO THE PRODUCT, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF NON-INFRINGEMENT, TITLE, MERCHANTABILITY, QUITE ENJOYMENT, QUALITY OF INFORMATION, AND FITNESS FOR A PARTICULAR PURPOSE. SOFTWARE AG AND ITS AFFILIATES AND LICENSORS DO NOT WARRANT THAT THE PRODUCT WILL MEET YOUR REQUIREMENTS, OR THAT THE OPERATION OF THE PRODUCT WILL BE UNINTERRUPTED OR ERROR-FREE, OR THAT DEFECTS IN THE PRODUCT WILL BE CORRECTED. NO ORAL OR WRITTEN INFORMATION OR ADVICE GIVEN BY SOFWARE AG OR ANY OF ITS PERSONNEL OR AGENTS WILL CREATE ANY WARRANTIES OR IN ANY WAY INCREASE THE SCOPE OF SOFTWARE AG’S OBLIGATIONS UNDER THIS AGREEMENT.
You assume full responsibility for the selection of the Product to achieve your intended results and for the installation, use and results obtained from the Product. Any kind of support for the Software AG Product is explicitly excluded.

### UPDATES AND MAINTENANCE

This Limited Use License Agreement does not grant you any right to, license for or interest in any improvements, modifications, enhancements or updates to the Product and documentation or other support services. Such services are typically available under a Commercial License Agreement only. Any such arrangements shall be the subject of a separate written agreement.

### LIMITATION OF LIABILITY

TO THE MAXIMUM EXTENT PERMITTED BY LAW, IN NO EVENT WILL SOFTWARE AG OR ITS AFFILIATES OR LICENSORS BE LIABLE TO YOU OR ANY THIRD PARTY FOR ANY DIRECT, SPECIAL, INCIDENTAL, CONSEQUENTIAL, PUNITIVE, OR INDIRECT DAMAGES, WHICH SHALL INCLUDE, WITHOUT LIMITATION, DAMAGES FOR PERSONAL INJURY, LOST PROFITS, LOST DATA AND BUSINESS INTERRUPTION, ARISING OUT OF THE USE OR INABILITY TO USE THE PRODUCT OR ANY SUPPORT SERVICES OR OTHER SERVICES, EVEN IF SOFTWARE AG HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGES. IN ANY CASE, THE ENTIRE AGGREGATE LIABILITY OF SOFTWARE AG AND ITS AFFILAITES AND LICENSORS UNDER THIS AGREEMENT FOR ALL DAMAGES, LOSSES, AND CAUSES OF ACTION (WHETHER IN CONTRACT, TORT (INCLUDING NEGLIGENCE), OR OTHERWISE) WILL BE LIMITED TO FEES PAID BY YOU, IF ANY, DURING THE THREE (3) MONTHS IMMEDIATELY PRECEEDING THE FIRST EVENT GIVING RISE TO LIABILITY.
THE LIMITATIONS OF LIABILITY AND DISCLAIMERS OF WARRANTIES PROVIDED IN THIS AGREEMENT FORM AN ESSENTIAL BASIS OF THE BARGAIN BETWEEN THE PARTIES AND SHALL CONTINUE TO APPLY EVEN IF ANY REMEDY HEREUNDER FAILS OF ITS ESSENTIAL PURPOSE.

### TERMINATION

This Limited Use License Agreement will terminate immediately without notice from Software AG if you fail to comply with any provision of this Limited License Agreement. Software AG reserves the right to terminate this agreement immediately for good cause, whereby good cause is understood as any breach of this agreement. In addition, Software AG may terminate this Agreement on written or electronic notice to you in the event the Product becomes the subject of an infringement claim or if it no longer has sufficient rights to license the Product. Upon termination, the license granted in this Agreement will automatically terminate and you must immediately discontinue the use of the Product and destroy the Product and all copies of the Product in physical, electronic or other form. Upon request of Software AG licensee will certify in written that use is discontinued and all copies of the Product are destroyed.
The following provisions will survive any termination or expiration of this Agreement: Intellectual Property, Confidentiality, Limitation of Liability, Export and Miscellaneous.

### EXPORT

You may not download or otherwise export or re-export any underlying software, technology or other information from the Products except as stated explicitly in this notice or the Commercial License Agreement and in full compliance with all applicable national and international laws and regulations. You agree to indemnify and hold harmless and defend Software AG against any and all liability arising from or relating to your breach of these export control undertakings. Software AG reserves the right not to honor any affected parts of this notice, or the Commercial License Agreement, in case any national or international export regulations or foreign trade legislation, or any target country / customer / usage restrictions implied by embargos or other sanctions prohibit the provision of export controlled goods (Dual-Use items) and services to be granted to you under either this notice or the Commercial License Agreement. Software AG may inform you if a related official export approval by national or international export control authorities is required. Provision of affected Products will then be postponed until all such required approvals have been granted. The provision of Products not restricted by the above mentioned export prohibitions will remain unaffected of this restriction.

### MISCELLANEOUS

The Product is designed for general office use. It is not designed or intended for use in air traffic control, mass transit systems, critical medical purposes, the operation of nuclear facilities or any other use which could result in a high risk of safety or property damage. You warrant that you will not use the Product for such purposes.
It is a material term that you shall not use the Product for benchmarking or similar performance-related testing purposes without the express written consent of Software AG. If Software AG consents to your using the Product for any benchmarking or similar performance-related testing purposes, you shall not publish or disclose to a third party the outcomes or results of any such exercise, or any information derived from the outcomes or results of such exercise, without the additional express written consent of Software AG.
The invalidity of any provision of this Agreement shall not affect any other part of this Agreement. This Agreement represents the complete and exclusive understanding between the parties. No modification or amendment of this Agreement will be binding on any party unless acknowledged in writing by their duly authorized representatives.
This Agreement is governed by the laws of the State of New York without giving effect to its conflicts-of-laws provisions and excluding the United Nations Convention on Contracts for the International Sale of Goods (CISG) and the Uniform Commercial Code (UCC). The parties consent to exclusive personal jurisdiction in federal and state courts located in the Southern District of New York. In the event a dispute arising under this Agreement results in litigation, the non-prevailing party will pay the court costs and reasonable attorneys’ fees and expenses of the prevailing Party. EACH PARTY WAIVES ALL RIGHT TO A JURY TRIAL IN ANY PROCEEDING ARISING OUT THIS AGREEMENT.
