## Getting started

3.7.0 release : clustered loader writer

As usual, it contains numerous [bug fixes and enhancements](https://github.com/ehcache/ehcache3/milestone/3.7.0?closed=1).

Ehcache ${version} has been released to maven central under the following coordinates:

### Main module

``` xml
<dependency>
  <groupId>org.ehcache</groupId>
  <artifactId>ehcache</artifactId>
  <version>3.7.0</version>
</dependency>
```

### Transactions module

``` xml
<dependency>
  <groupId>org.ehcache</groupId>
  <artifactId>ehcache-transactions</artifactId>
  <version>3.7.0</version>
</dependency>
```

### Clustering module

``` xml
<dependency>
  <groupId>org.ehcache</groupId>
  <artifactId>ehcache-clustered</artifactId>
  <version>3.7.0</version>
</dependency>
```

Or can be downloaded below.
Note that if you download Ehcache jar you will need one additional jar in your classpath:
- [slf4j-api-1.7.25.jar](http://search.maven.org/#artifactdetails%7Corg.slf4j%7Cslf4j-api%7C1.7.25%7Cjar)

## Clustering kit

For clustering a kit is also provided that includes the Terracotta Server component. See below.

## Further reading
- [Ehcache 3 documentation](http://www.ehcache.org/documentation/3.7/)
