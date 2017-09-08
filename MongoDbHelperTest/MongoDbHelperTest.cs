using Microsoft.VisualStudio.TestTools.UnitTesting;
using MongoDB.Driver;
using MongoDbHelper;
using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace MongoDbHelperTest
{
    [TestClass()]
    public class MongoDBHelperTests
    {
        string connectionString = "mongodb://127.0.0.1:27018";
        string database = "MingTest";
        string document = "MingDoc";
        string id = "59a7c37349b7888ccfd9e1a5";
        MongoDBHelper db = null;
        [TestInitialize]
        public void MongoDBHelperTest()
        {
            db = new MongoDBHelper(connectionString, database);
        }

        private TestEntity GetEntity()
        {
            Random rad = new Random();
            TestEntity te = new TestEntity();
            te.OrderId = Guid.NewGuid().ToString("N");
            te.CreateDate = DateTime.Now;
            te.CustomerId = rad.Next(100).ToString();
            te.CustomerName = rad.Next(100).ToString();
            te.Note = rad.Next(100).ToString();
            te.Qty = rad.Next(100);
            te.OrderDate = DateTime.Now;
            te.OrderName = rad.Next(100).ToString();
            return te;
        }

        [TestMethod()]
        public void InsertTestMethod()
        {
            for (int i = 0; i < 10; i++)
            {
                TestEntity te = GetEntity();
                db.Insert<TestEntity>(document, te);
            }
        }

        [TestMethod()]
        public void InsertTestMethodMany()
        {
            IList<TestEntity> lst = new List<TestEntity>();
            for (int i = 0; i < 100; i++)
            {
                TestEntity te = GetEntity();
                lst.Add(te);
            }
            db.InsertMany<TestEntity>(document, lst);
        }

        [TestMethod()]
        public void IsExistDocumentTest()
        {
            Assert.Fail();
        }

        [TestMethod()]
        public void GetCountTest()
        {
            var filter = Builders<TestEntity>.Filter.Empty;
            long n = db.GetCount<TestEntity>(document, filter);
            Debug.WriteLine(n);
            //db.GetCount<TestEntity>()

        }

        [TestMethod()]
        public void GetDocumentByIdTest()
        {
            TestEntity te = db.GetDocumentById<TestEntity>(document, "59a7c37349b7888ccfd9e1a5");
            Console.WriteLine(te.OrderId);
        }

        [TestMethod()]
        public void GetDocumentByIdFiledTest()
        {
            var project = Builders<TestEntity>.Projection.Include("OrderId").Include("Note");
            TestEntity te = db.GetDocumentById<TestEntity>(document, "59a7c37349b7888ccfd9e1a5", project);
            Assert.AreEqual(te.Note, "81");
        }

        [TestMethod()]
        public void GetAllDocuments()
        {
            IList<TestEntity> te = db.GetAllDocuments<TestEntity>(document);
            foreach (var item in te)
            {
                System.Diagnostics.Debug.WriteLine(item.OrderId);
            }
        }

        [TestMethod()]
        public void GetAllDocuments2()
        {
            var project = Builders<TestEntity>.Projection.Include("OrderId").Include("Note");
            IList<TestEntity> te = db.GetAllDocuments<TestEntity>(document, project);

            foreach (var item in te)
            {
                System.Diagnostics.Debug.WriteLine(item.OrderId);
            }
        }

        [TestMethod()]
        public void GetDocumentByUserFilter()
        {
            var fields = Builders<TestEntity>.Projection.Include("OrderId").Include("Note");
            var filter = Builders<TestEntity>.Filter.Eq("id", id);
            TestEntity te = db.GetDocumentByUserFilter<TestEntity>(document, filter, fields);
            System.Diagnostics.Debug.WriteLine(te.OrderId);

        }

        [TestMethod()]
        public void GetAllDocumentsTest()
        {
           
        }

        [TestMethod()]
        public void GetAllDocumentsTest1()
        {
           
        }

        [TestMethod()]
        public void GetDocumentsByFilterTest()
        {
          
        }

        [TestMethod()]
        public void GetDocumentsByFilterTest1()
        {
            var filter = Builders<TestEntity>.Filter.Eq("Qty", "7");
            var t = db.GetDocumentsByFilter<TestEntity>(document, filter);
            foreach (var item in t)
            {
                Debug.WriteLine(item.OrderDate);
                Debug.WriteLine(item.Qty);
            }
        }

        [TestMethod()]
        public void GetDocumentsByFilterTest2()
        {
            var t = db.GetDocumentsByFilter<TestEntity>(document, "Qty", "7");
            foreach (var item in t)
            {
                Debug.WriteLine(item.OrderDate);
                Debug.WriteLine(item.Qty);
            }
        }

        [TestMethod()]
        public void GetDocumentsByFilterTest3()
        {
            var filter = Builders<TestEntity>.Filter.Eq("Qty", "7");
            var fields = Builders<TestEntity>.Projection.Include("OrderId").Include("Note");
            var t = db.GetDocumentsByFilter<TestEntity>(document, filter, fields);
            foreach (var item in t)
            {
                Debug.WriteLine(item.OrderId);
                Debug.WriteLine(item.Note);
                Debug.WriteLine(item.OrderDate);
            }
        }

        [TestMethod()]
        public void GetDocumentsByFilterTest4()
        {
            var fields = Builders<TestEntity>.Projection.Include("OrderId").Include("Note");
            var t = db.GetDocumentsByFilter<TestEntity>(document, "Qty", "7", fields);
            foreach (var item in t)
            {
                Debug.WriteLine(item.OrderId);
                Debug.WriteLine(item.Note);
                Debug.WriteLine(item.OrderDate);
            }
        }

        [TestMethod()]
        public void GetPagedDocumentsByFilterTest1()
        {
            var t = db.GetPagedDocumentsByFilter<TestEntity>(document, 1, 20);
            int i = 0;
            foreach (var item in t)
            {
                Debug.WriteLine(item.OrderId);
                Debug.WriteLine(item.Note);
                Debug.WriteLine(item.OrderDate);
                Debug.WriteLine(++i);
            }
        }

        [TestMethod()]
        public void GetPagedDocumentsByFilterTest2()
        {
            var filter = Builders<TestEntity>.Filter.Eq("Qty", "7");
            var t = db.GetPagedDocumentsByFilter<TestEntity>(document, filter, 1, 10);
            int i = 0;
            foreach (var item in t)
            {
                Debug.WriteLine(item.OrderId);
                Debug.WriteLine(item.Note);
                Debug.WriteLine(item.OrderDate);
                Debug.WriteLine(++i);
            }
        }

        [TestMethod()]
        public void GetPagedDocumentsByFilterTest3()
        {
            var sort = Builders<TestEntity>.Sort.Descending("Qty");
            var t = db.GetPagedDocumentsByFilter<TestEntity>(document, sort, 1, 5);
            int i = 0;
            foreach (var item in t)
            {
                Debug.WriteLine(item.OrderId);
                Debug.WriteLine(item.Note);
                Debug.WriteLine(item.OrderDate);
                Debug.WriteLine("Qty " + item.Qty);
                Debug.WriteLine(++i);
            }
        }

        [TestMethod()]
        public void GetPagedDocumentsByFilterTest4()
        {
            var sort = Builders<TestEntity>.Sort.Descending("Note");
            var filter = Builders<TestEntity>.Filter.Eq("Qty", "7");
            var t = db.GetPagedDocumentsByFilter<TestEntity>(document, filter, sort, 1, 30);
            int i = 0;
            foreach (var item in t)
            {
                Debug.WriteLine("OrderId={0}, Note={1}, Qty={2}, i={3};", item.OrderId, item.Note, item.Qty, ++i);
            }
        }

        [TestMethod()]
        public void GetPagedDocumentsByFilterTest5()
        {
            var sort = Builders<TestEntity>.Sort.Descending("Note");
            var filter = Builders<TestEntity>.Filter.Eq("Qty", "7");
            var fields = Builders<TestEntity>.Projection.Include("OrderId").Include("Note");
            var t = db.GetPagedDocumentsByFilter<TestEntity>(document, filter, fields, sort, 1, 30);
            int i = 0;
            foreach (var item in t)
            {
                Debug.WriteLine("OrderId={0}, Note={1}, Qty={2}, i={3};", item.OrderId, item.Note, item.Qty, ++i);
            }
        }

        [TestMethod()]
        public void UpdateTest()
        {
            db.Update<TestEntity>(document, "59a7c35a49b7888ccfd9e187", "Qty", "27");
            var fiter = Builders<TestEntity>.Filter.Eq("Qty", "53");
            var update = Builders<TestEntity>.Update.Set("Note", "Hello");
            //db.Update<TestEntity>(document, fiter, update);
            // db.UpdateMany<TestEntity>(document, fiter, update);
            //db.UpdateMany<TestEntity>(document, "59a7c35a49b7888ccfd9e187", "Qty", "14");
        }

        [TestMethod]
        public void UpdateReplaceone()
        {
            TestEntity te = GetEntity();
            var builder = Builders<TestEntity>.Filter.Eq("Qty", 27);
            db.UpdateReplaceOne<TestEntity>(document, builder, te);
        }

        [TestMethod]
        public void UpdateReplacemany()
        {
            TestEntity te = GetEntity();
            db.UpdateReplaceOne<TestEntity>(document, "59a7c35a49b7888ccfd9e187", te);
        }


        [TestMethod()]
        public void UpdateTest1()
        {
            Assert.Fail();
        }

        [TestMethod()]
        public void DeleteTest()
        {
            db.Delete<TestEntity>(document, "59a7c29c49b7888ccfd9e157");
        }

        [TestMethod()]
        public void DeleteManyTest()
        {
            var filter = Builders<TestEntity>.Filter;
            FilterDefinition<TestEntity> filter1 = filter.And(filter.Eq("Qty", "68"), filter.Eq("Note", "54"));
            FilterDefinition<TestEntity> filter2 = filter.Eq("Qty", 54);
            db.DeleteMany<TestEntity>(document, "Qty", "93");
        }
    }
}
