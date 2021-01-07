const elastic = require('@elastic/elasticsearch');

const sourceIndex = 'test_1';
const destinationIndex = 'test_2';

const numberOfDocs = 100000;
const timeoutMs = 3000;

const updateDocId = 10000; // this document id will get updated after the initial reindex starts
const newDocId = 999999999; // this document id will get created after the initial reindex starts
const deleteDocId = 20000; // this document id will get deleted after the initial reindex starts

async function timeout(ms = timeoutMs) {
    return new Promise((res) => {
        setTimeout(() => res(), ms);
    });
}

const indexMapping = {
    properties: {
        name: {
            type: 'keyword'
        },
        age: {
            type: 'integer'
        }
    }
};

function setupEsClient() {
    return new elastic.Client({
        node: 'http://localhost:9200'
    });
}

async function createIndex(client, index) {
    await client.indices.create({
        index: index,
        body: {
            mappings: indexMapping
        }
    });
}

async function insertDocuments(client, index) {
    console.log('==== Inserting documents');
    let b = [];
    let i = 0;
    while (i++ < numberOfDocs) {
        b.push({ index: { _index: index, _id: i } });
        b.push({ name: 'a', age: i });

        if (i % 2500 === 0) {
            await client.bulk({ body: b });
            b = [];
        }
        if (i % 10000 === 0) {
            console.log(`Wrote ${i} docs`);
        }
    }
}

async function readDocument(client, fromIndex, id) {
    console.log(`==== Reading document from index (${fromIndex}): ${id} ====`);

    try {
        const doc = await client.get({
            index: fromIndex,
            id: id
        });

        console.log(' --- Document found! ');
        console.log(JSON.stringify(doc.body));
    } catch (err) {
        console.log(' --- ERROR: Document read error!');
        console.log(JSON.stringify(err));
    }
}

async function opsWhileReindexInProcess(client) {
    console.log('==== Running extra operations while reindex in process ====')
    try {
        await Promise.all([
            createNewDocument(client),
            deleteExistingDocument(client),
            updateExistingDocument(client)
        ]);
        console.log(' --- Documents created, updated, and deleted');
    } catch (err) {
        console.log(' --- ERROR: While running operations');
        console.log(JSON.stringify(err));
    }
}

async function createNewDocument(client) {
    return await client.index({
        index: sourceIndex,
        id: newDocId,
        body: {
            name: 'garbage additional data',
            age: 9876
        }
    });
}

async function deleteExistingDocument(client) {
    return await client.delete({
        index: sourceIndex,
        id: deleteDocId
    });
}

async function updateExistingDocument(client) {
    return await client.index({
        index: sourceIndex,
        id: updateDocId,
        body: {
            name: 'document updated data',
            age: 111111111
        }
    });
}

async function refreshIndex(client, index) {
    return await client.indices.refresh({
        index
    });
}

async function reindex(client, phase) {
    console.log(`==== Starting reindex phase: ${phase}`);

    if (phase === 1) {
        // phase 1 create the destination index
        await client.indices.create({
            index: destinationIndex,
            body: {
                mappings: indexMapping
            }
        });

        await client.indices.putSettings({
            index: destinationIndex,
            body: {
                refresh_interval: -1,
                number_of_replicas: 0
            }
        });

        const r = await client.reindex({
            body: {
                source: {
                    index: sourceIndex
                },
                dest: {
                    index: destinationIndex,
                    version_type: 'external'
                }
            },
            wait_for_completion: true,
            requests_per_second: 5000 // just to have it go slow enough to see the effects
        });
        console.log(JSON.stringify(r));

        // refresh the destination once we're done
        await refreshIndex(client, destinationIndex);

    } else {
        // phase 2, do it again with version_type=external
        try {
            const r = await client.reindex({
                body: {
                    source: {
                        index: sourceIndex
                    },
                    dest: {
                        index: destinationIndex,
                        version_type: 'external'
                    },
                    conflicts: 'proceed'
                },
                wait_for_completion: true,
                requests_per_second: 5000 // just to have it go slow enough to see the effects
            });
            console.log(JSON.stringify(r));
        } catch (err) {
            console.log(' ---  ERROR during phase 2');
            // console.log(JSON.stringify(err));
        }

        await client.indices.putSettings({
            index: destinationIndex,
            body: {
                refresh_interval: '1s',
                number_of_replicas: 1
            }
        });

        await refreshIndex(client, destinationIndex);
    }
}

async function main() {

    const client = setupEsClient();

    console.log('\n0. Setup -- creation, indexing source, refresh.');
    await createIndex(client, sourceIndex);
    await insertDocuments(client, sourceIndex);
    await refreshIndex(client, sourceIndex);

    // read out the initial docs
    console.log('\n1. Reading initial documents');
    await readDocument(client, sourceIndex, updateDocId); // should exist
    await readDocument(client, sourceIndex, deleteDocId); // should exist
    await readDocument(client, sourceIndex, newDocId); // should not exist yet

    // start the reindex but don't wait on the promise yet
    console.log('\n2. Starting reindex');
    const reindexPromise = reindex(client, 1);

    // perform extra operations
    console.log('\n3. Doing extra operations');
    // pauses enough time for the phase 1 reindex to take its snapshot of the source index.
    await timeout();

    // then do those things which affects the initial index
    // this simulates real time operations while the system is totally up.
    await opsWhileReindexInProcess(client);

    // wait on the reindex promise
    console.log('\n4. Waiting on reindex to complete');
    await reindexPromise;

    // read documents again from source index
    console.log('\n5. Reading documents from source to see edits.');
    await readDocument(client, sourceIndex, updateDocId); // should have been edited
    await readDocument(client, sourceIndex, deleteDocId); // should have been deleted
    await readDocument(client, sourceIndex, newDocId); // should have been created 

    // then read from destination index
    console.log('\n6. Reading documents from destination to see missing changes.');
    await readDocument(client, destinationIndex, updateDocId); // will not have the edits
    await readDocument(client, destinationIndex, deleteDocId); // will not have been deleted
    await readDocument(client, destinationIndex, newDocId);  // will not have been created

    // start the second reindex with version=external
    console.log('\n7. Reindex phase 2.');
    // phase 2 reindex will make sure it applies updates & creations from the original, as long 
    // as the version is less than or equal -- i.e., not overwrite edits to the destination.
    await reindex(client, 2);
    //

    // then read from destination index a second time to see fixed issues
    console.log('\n8. Reading documents from destination after phase 2');
    await readDocument(client, destinationIndex, updateDocId); // now this document has been replayed with the edits
    await readDocument(client, destinationIndex, deleteDocId); // THIS DOCUMENT WILL NOT BE DELETED
    await readDocument(client, destinationIndex, newDocId); // now this document exists

}

console.log('Starting reindex test...');
console.log(new Date().toISOString());
main().then(
    () => {
        console.log('Completed test...');
        console.log(new Date().toISOString());
    },
    (err) => {
        console.error('==== ERROR DURING TESTS! ++++ ');
        console.error(JSON.stringify(err));
        console.log(new Date().toISOString());
    });

