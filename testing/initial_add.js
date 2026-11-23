import { assert } from 'chai';
import { Mongo } from 'meteor/mongo';

const InitialAddCollection = new Mongo.Collection('initial_add');
describe('Initial Add', function () {
  let lastDocId;
  before(async function () {
    await InitialAddCollection.removeAsync({});
    for (let i = 0; i <= 10; i++) {
      lastDocId = await InitialAddCollection.insertAsync({ number: i });
    }
  });

  it('Should not crash on initial add', function (done) {
    Meteor.defer(async () => {
      let err;
      await InitialAddCollection.find().observeChanges({
        async added(_id, doc) {
          if (err) return;
          await Meteor._sleepForMs(10); // simulate a more costly operation
          try {
            assert.isDefined(doc);
          } catch (e) {
            err = e;
          }
        },
      });
      done(err);
    });
    Meteor.defer(async () => {
      await InitialAddCollection.removeAsync({ _id: lastDocId });
    });
  });

  it('keeps a live query when a new handle joins just before its last handle stops', async function () {
    const id = await InitialAddCollection.insertAsync({ number: 42 });
    const cursor = InitialAddCollection.find(id);
    const first = await cursor.observeChanges({ added() {} });
    let replacement;
    try {
      const added = [];
      const joining = cursor.observeChanges({ added(docId) { added.push(docId); } });
      [replacement] = await Promise.all([joining, first.stop()]);
      assert.deepEqual(added, [id]);
    } finally {
      if (replacement) await replacement.stop();
      await first.stop();
      await InitialAddCollection.removeAsync(id);
    }
  });
});
