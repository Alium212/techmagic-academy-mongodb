import { connect, close } from './connection.js';

const db = await connect();
const usersCollection = db.collection("users");
const articlesCollection = db.collection('articles');
const studentsCollection = db.collection('students');

const run = async () => {
  try {
    await getUsersExample();
    await task1();
    await task2();
    await task3();
    await task4();
    await task5();
    await task6();
    await task7();
    await task8();
    await task9();
    await task10();
    await task11();
    await task12();

    await close();
  } catch(err) {
    console.log('Error: ', err)
  }
}
run();

// #### Users
// - Get users example
async function getUsersExample () {
  try {
    const [allUsers, firstUser] = await Promise.all([
      usersCollection.find().toArray(),
      usersCollection.findOne(),
    ])

    // console.log('allUsers', allUsers);
    // console.log('firstUser', firstUser);
    return allUsers;
  } catch (err) {
    console.error('getUsersExample', err);
  }
}

// - Get all users, sort them by age (ascending), and return only 5 records with firstName, lastName, and age fields.
async function task1 () {
  try {
    const allUsers = await getUsersExample();
    const sortedUsers = allUsers.sort((a, b) => a.age - b.age);

    const limitedUsers = sortedUsers.slice(0, 5).map(user => ({
      firstName: user.firstName,
      lastName: user.lastName,
      age: user.age
    }));

    console.log('All users, sort them by age (ascending): ', limitedUsers)
    return limitedUsers;
  } catch (err) {
    console.error('task1', err);
  }
}

// - Add new field 'skills: []" for all users where age >= 25 && age < 30 or tags includes 'Engineering'
async function task2 () {
  try {
    await usersCollection.updateMany(
      {
        $or: [
          { $and: [{ age: { $gte: 25 } }, { age: { $lt: 30 } }] },
          { tags: 'Engineering' }
        ]
      },
      {
        $set: { skills: [] }
      }
    );

    console.log('Skills field added to qualifying users.');
  } catch (err) {
    console.error('task2', err)
  }
}

// - Update the first document and return the updated document in one operation (add 'js' and 'git' to the 'skills' array)
//   Filter: the document should contain the 'skills' field
async function task3() {
  try {
    const filter = { skills: { $exists: true } };
    const updatedDocument = await usersCollection.findOneAndUpdate(
      filter,
      { $push: { skills: { $each: ['js', 'git'] } } },
      { returnOriginal: false }
    );

    console.log('Updated document:', updatedDocument);
    return updatedDocument;
  } catch (err) {
    console.error('task3', err)
  }
}

// - REPLACE the first document where the 'email' field starts with 'john' and the 'address state' is equal to 'CA'
//   Set firstName: "Jason", lastName: "Wood", tags: ['a', 'b', 'c'], department: 'Support'
async function task4 () {
  try {
    const filter = {
      email: { $regex: /^john/i },
      'address.state': 'CA'
    };

    const replacement = {
      firstName: 'Jason',
      lastName: 'Wood',
      tags: ['a', 'b', 'c'],
      department: 'Support'
    };

    const replacedDocument = await usersCollection.findOneAndReplace(filter, replacement, {
      returnOriginal: false
    });

    console.log('Replaced document:', replacedDocument);
    return replacedDocument;
  } catch (err) {
    console.log('task4', err);
  }
}

// - Pull tag 'c' from the first document where firstName: "Jason", lastName: "Wood"
async function task5 () {
  try {
    const filter = {
      firstName: 'Jason',
      lastName: 'Wood'
    };

    const updatedDocument = await usersCollection.findOneAndUpdate(
      filter,
      { $pull: { tags: 'c' } },
      { returnOriginal: false }
    );

    console.log('Updated document:', updatedDocument);
    return updatedDocument;
  } catch (err) {
    console.log('task5', err);
  }
}

// - Push tag 'b' to the first document where firstName: "Jason", lastName: "Wood"
//   ONLY if the 'b' value does not exist in the 'tags'
async function task6 () {
  try {
    const filter = {
      firstName: 'Jason',
      lastName: 'Wood'
    };

    const updatedDocument = await usersCollection.findOneAndUpdate(
      filter,
      { $addToSet: { tags: 'b' } },
      { returnOriginal: false }
    );

    console.log('Updated document:', updatedDocument);
    return updatedDocument;
  } catch (err) {
    console.log('task6', err);
  }
}

// - Delete all users by department (Support)
async function task7 () {
  try {
    const filter = {
      department: 'Support'
    };

    const deletionResult = await usersCollection.deleteMany(filter);

    console.log(`Deleted ${deletionResult.deletedCount} documents.`);
    return deletionResult;
  } catch (err) {
    console.log('task7', err);
  }
}

// #### Articles
// - Create new collection 'articles'. Using bulk write:
//   Create one article per each type (a, b, c)
//   Find articles with type a, and update tag list with next value ['tag1-a', 'tag2-a', 'tag3']
//   Add tags ['tag2', 'tag3', 'super'] to articles except articles with type 'a'
//   Pull ['tag2', 'tag1-a'] from all articles
async function task8 () {
  try {
    const collection = db.collection('articles'); // Replace 'articles' with your collection name

    const operations = [
      { insertOne: { document: { type: 'a' } } },
      { insertOne: { document: { type: 'b' } } },
      { insertOne: { document: { type: 'c' } } },
      {
        updateOne: {
          filter: { type: 'a' },
          update: { $set: { tags: ['tag1-a', 'tag2-a', 'tag3'] } }
        }
      },
      {
        updateMany: {
          filter: { type: { $ne: 'a' } },
          update: { $addToSet: { tags: { $each: ['tag2', 'tag3', 'super'] } } }
        }
      },
      {
        updateMany: {
          filter: {},
          update: { $pull: { tags: { $in: ['tag2', 'tag1-a'] } } }
        }
      }
    ];

    const result = await collection.bulkWrite(operations);

    console.log('Result:', result);
    return result;
  } catch (err) {
    console.error('task8', err);
  }
}

// - Find all articles that contains tags 'super' or 'tag2-a'
async function task9 () {
  try {
    const filter = {
      $or: [
        { tags: 'super' },
        { tags: 'tag2-a' }
      ]
    };

    const articles = await articlesCollection.find(filter).toArray();

    console.log('Articles with tags "super" or "tag2-a":', articles);
    return articles;
  } catch (err) {
    console.log('task9', err);
  }
}

// #### Students Statistic (Aggregations)
// - Find the student who have the worst score for homework, the result should be [ { name: <name>, worst_homework_score: <score> } ]
async function task10 () {
  try {
    const result = await studentsCollection.aggregate([
      { $unwind: '$scores' },
      { $match: { 'scores.type': 'homework' } },
      {
        $group: {
          _id: '$name',
          worst_homework_score: { $min: '$scores.score' }
        }
      },
      { $sort: { worst_homework_score: 1 } },
      { $limit: 1 },
      {
        $project: {
          _id: 0,
          name: '$_id',
          worst_homework_score: 1
        }
      }
    ]).toArray();

    console.log('Student with the worst homework score: ', result);
    return result;
  } catch (err) {
    console.log('task10', err);
  } 
}

// - Calculate the average score for homework for all students, the result should be [ { avg_score: <number> } ]
async function task11 () {
  try {
    const result = await studentsCollection.aggregate([
      { $unwind: '$scores' },
      { $match: { 'scores.type': 'homework' } },
      {
        $group: {
          _id: null,
          avg_score: { $avg: '$scores.score' }
        }
      },
      {
        $project: {
          _id: 0,
          avg_score: 1
        }
      }
    ]).toArray();

    console.log('Average score for homework: ', result);
    return result;
  } catch (err) {
    console.log('task11', err);
  } 
}

// - Calculate the average score by all types (homework, exam, quiz) for each student, sort from the largest to the smallest value
async function task12 () {
  try {
    const result = await studentsCollection.aggregate([
      { $unwind: '$scores' },
      {
        $group: {
          _id: { name: '$name', type: '$scores.type' },
          avg_score: { $avg: '$scores.score' }
        }
      },
      {
        $group: {
          _id: '$_id.name',
          scores: { $push: { type: '$_id.type', avg_score: '$avg_score' } }
        }
      },
      {
        $project: {
          _id: 0,
          name: '$_id',
          scores: 1,
          total_avg_score: {
            $avg: '$scores.avg_score'
          }
        }
      },
      { $sort: { total_avg_score: -1 } }
    ]).toArray();

    console.log('Average score by all types for each student: ', result);
    return result;
  } catch (err) {
    console.log('task12', err);
  } 
}
