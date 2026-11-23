import { DDP } from 'meteor/ddp-client';
import { Meteor } from 'meteor/meteor';
import { Mongo } from 'meteor/mongo';

window.crossInstance = {
    items: new Mongo.Collection('cross_instance_items'),
    probes: new Mongo.Collection('cross_instance_probes'),
    subscribe: (name, ...args) => Meteor.subscribe(name, ...args),
    callReader: (name, ...args) => Meteor.callAsync(name, ...args),
    connectWriter: url => DDP.connect(url),
};
