#PostGreSQL evaluation

## General impressions
* Easy to set up
* Good standard SQL support
* Well documented
* Reactive API in Java
* Awesome Network types, inet, cidr and mac

## Load testing

### Minimal Event Schema
#### Load Performance (using COPY)
With no indices: 600,000+ row/sec!

With 2 indices:
    10,000-30,000 rows/sec....
    
What about parallelism?  No effect when sharing the same connection
 