# destination-mongodb

MongoDB Adapter for the Destination Framework

### Settings

```js
{
  // MongoDB Host
  host: '127.0.0.1',
  
  // Not required when default port
  port: '27017',
  
  // Authentication, Optional
  username: 'name',
  password: 'word',
  
  // Database Name
  database: 'test',
  
  // Options
  options: {
    // MongoDB Native Adapter options go here.
  }
}
```

or

```js
{
  urls: [
    // List of mongodb uris.
  ]
}
```
