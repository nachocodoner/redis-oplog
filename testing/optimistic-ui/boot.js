import { Meteor } from 'meteor/meteor';
import { Items } from './collections';

if (Meteor.isServer) {
    Meteor.publish('optimistic_ui.items', function(...args) {
        return Items.find(...args);
    });
}

Items.allow({
    insertAsync: () => true,
    updateAsync: () => true,
    removeAsync: () => true,
    insert: () => true,
    update: () => true,
    remove: () => true,
});

Meteor.methods({
    'optimistic_ui.items.insert'(doc) {
        if (Meteor.isClient) return Items.insert(doc);
        return Items.insert(doc, { optimistic: true });
    },
    'optimistic_ui.items.update'(selector, modifier) {
        if (Meteor.isClient) return Items.update(selector, modifier);
        return Items.update(selector, modifier, { optimistic: true });
    },
    'optimistic_ui.items.remove'(selector) {
        if (Meteor.isClient) return Items.remove(selector);
        return Items.remove(selector, { optimistic: true });
    },
});
