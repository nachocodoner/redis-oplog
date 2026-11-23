Package.describe({
    name: 'local:observer-check',
    version: '0.0.1',
    summary: 'Local observer comparison fixture',
});

Package.onTest(function (api) {
    api.use(['ecmascript', 'mongo', 'tinytest', 'random']);
    api.addFiles('driver.js', 'server');
});
