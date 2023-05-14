import re
import pprint

from bson import ObjectId
from pydantic import BaseModel, Field
from typing_extensions import Annotated
from motor.motor_asyncio import AsyncIOMotorClient

from fastapi.encoders import jsonable_encoder
from fastapi.responses import Response, JSONResponse
from fastapi import FastAPI, Body, status, Query, HTTPException


app = FastAPI()

###
# Connecting to MongoDB using Motor asynchronously. 
# Under the hood, Motor uses PyMongo in ThreadPoolExecutor 
# Hence, using Motor as the requirement states the connection should be asynchronous and
# PyMongo is Synchronous/Blocking
###

####
# Mongo Atlas Cluster Credentials
###
User = "username"
Pass = "password"

####
# Mongo Connection using Motor
###
mongo_uri = f"mongodb+srv://{User}:{Pass}@bookstore.kf0k64l.mongodb.net/?retryWrites=true&w=majority"
client = AsyncIOMotorClient(mongo_uri)
db = client.Bookstore
collection = db['books']

####
# Creating a serializable ObjectID
###
class PyObjectId(ObjectId):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not ObjectId.is_valid(v):
            raise ValueError("Invalid objectid")
        return ObjectId(v)

    @classmethod
    def __modify_schema__(cls, field_schema):
        field_schema.update(type="string")

###
# Pydantic model for the book data
###

class BookModel(BaseModel):
    id: PyObjectId = Field(default_factory=PyObjectId, alias="_id")
    title: str = Field(title="The name of the book", max_length=100)
    author: str = Field(title="The name of the author", max_length=100)
    description: str = Field(title="A short description of the book", max_length=300)
    price: float = Field(title="A price of the book", ge=0.0)
    stock: int = Field(title="Number of books left in stock", ge=0)
    sold: int = Field(default=0, title="Number of books sold", ge=0)

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}
        schema_extra = {
            "example": {
                "title": "Introduction to Web Backend",
                "author": "Jane Doe",
                "description": "A simple introduction to Web Backend Development",
                "price": 50.00,
                "stock": 3,
                "sold": 10
            }
        }

###
# Pydantic model for the book data when updating the mongo collection
# Here id attribute is removed since the id value once auto-generated must never be changed
# and all fields are optional as only required fields need to be updated
###

class UpdateBookModel(BaseModel):
    title: str | None = Field( default=None, title="The name of the book", max_length=100)
    author: str | None  = Field( default=None, title="The name of the author", max_length=100)
    description: str | None = Field( default=None, title="A short description of the book", max_length=300)
    price: float | None = Field( default=None, title="A price of the book", ge=0.0)
    stock: int | None = Field(default=None, title="Number of books left in stock", ge=0)

    class Config:
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}
        schema_extra = {
            "example": {
                "title": "Introduction to Web Backend",
                "author": "Jane Doe",
                "description": "A simple introduction to Web Backend Development",
                "price": 50.00,
                "stock": 3,
                "sold": 10
            }
        }


###
# Health Check API
###
@app.get("/")
def index():
    return {"status": "Healthy"}

###
# Add a new book to the bookstore
###
@app.post("/books", response_description="Add new Book", response_model= BookModel)
async def create_book(book: BookModel = Body(...)):
    book = jsonable_encoder(book)
    new_book = await collection.insert_one(book)
    created_book = await collection.find_one({"_id": new_book.inserted_id})
    return JSONResponse(status_code=status.HTTP_201_CREATED, content=created_book)

###
# List all books in the bookstore
# Paginated to 20 results per page
###
@app.get("/books", response_description="List all Books", response_model=list[BookModel])
async def list_books(page : Annotated[int, Query(gt=0)] = 1):
    limit=20
    skip = (page - 1) * limit

    return await collection.find(skip = skip, limit = limit).sort('title').to_list(limit)

###
# Find a book in the bookstore with its ID
# If book with ID is not found, throw an exception
###
@app.get("/books/{book_id}", response_description="Fetch an existing Book by ID", response_model=BookModel)
async def get_a_book(book_id : str):
    if (book := await collection.find_one({"_id": book_id})) is not None:
        return book
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Book {book_id} not found")

###
# Update a book in the bookstore with its ID
###
@app.put("/books/{book_id}", response_description="Update an existing Book by ID", response_model=BookModel)
async def get_a_book(book_id : str, book: UpdateBookModel = Body(...)):

    # Removing keys-value for fileds that have None value
    book = {k:v for k, v in book.dict().items() if v is not None}

    # If there is any fields to update after the previous step
    if len(book) >= 1:
        update_result = await collection.update_one({"_id": book_id}, {"$set": book})
        if update_result.modified_count == 1:
            if (updated_book := await collection.find_one({"_id": book_id})) is not None:
                return updated_book

    # If nothing was changed, modified count will NOT be 1. Hence displaying existing book
    if (existing_book := await collection.find_one({"_id": book_id})) is not None:
        return existing_book

    # If book does not exist in book store
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Book {book_id} not found")

###
# Delete a book in the bookstore with its ID
###
@app.delete("/books/{book_id}", response_description="Delete a Book")
async def delete_book(book_id : str):
    delete_result = await collection.delete_one({"_id": book_id})

    if delete_result.deleted_count == 1:
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Book {book_id} not found")

###
# List all books filtered by title, author, and price range in the bookstore
# Search is Regex based so that case insensitive and incomplete book names will also fetch results
# Paginated to 20 results per page
###
@app.get("/search", response_description="List Books by title", response_model=list[BookModel])
async def list_books_by_title(title : str,
                             author : str,
                            min_price : Annotated[int, Query(ge=0)] = 0,
                           max_price : Annotated[int, Query(gt=0)] = 1000,
                          page : Annotated[int, Query(gt=0)] = 1):
    # min should not be greater than max. Throwing exception
    if min_price > max_price:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                           detail=f"lower price limit (min = {min_price}) cannot not be greater than upper price limit (max = {max_price})")
    
    limit=20
    skip = (page - 1) * limit

    books = await collection.find({"title": re.compile(title, re.IGNORECASE),
                                   "author": re.compile(author, re.IGNORECASE),
                                   "price": {"$gte" : min_price, "$lte" : max_price}
                                   }, skip = skip, limit = limit).sort('title').to_list(limit)
    return books

###
# List all books filtered by title in the bookstore
# Search is Regex based so that case insensitive and incomplete book names will also fetch results
# Paginated to 20 results per page
###
@app.get("/search/title/{title}", response_description="List Books by title", response_model=list[BookModel])
async def list_books_by_title(title : str, page : Annotated[int, Query(gt=0)] = 1):
    limit=20
    skip = (page - 1) * limit

    books = await collection.find({"title": re.compile(title, re.IGNORECASE)}, skip = skip, limit = limit).sort('title').to_list(limit)
    return books

###
# List all books filtered by author in the bookstore
# Search is Regex based so that case insensitive and incomplete author names will also fetch results
# Paginated to 20 results per page
###
@app.get("/search/author/{author}", response_description="List Books by author", response_model=list[BookModel])
async def list_books_by_author(author : str, page : Annotated[int, Query(gt=0)] = 1):
    limit=20
    skip = (page - 1) * limit

    books = await collection.find({"author": re.compile(author, re.IGNORECASE)}, skip = skip, limit = limit).sort('author').to_list(limit)
    return books

###
# List all books filtered by price range in the bookstore
# Search is Regex based so that case insensitive and incomplete book names will also fetch results
# Paginated to 20 results per page
###
@app.get("/search/price", response_description="List Books by price range", response_model=list[BookModel])
async def list_books_by_price(min_price : Annotated[int, Query(ge=0)] = 0,
                             max_price : Annotated[int, Query(gt=0)] = 1000,
                            page : Annotated[int, Query(gt=0)] = 1):
    
    # min should not be greater than max. Throwing exception
    if min_price > max_price:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                           detail=f"lower price limit (min = {min_price}) cannot not be greater than upper price limit (max = {max_price})")
    
    limit=20
    skip = (page - 1) * limit

    books = await collection.find({"price": {"$gte" : min_price, "$lte" : max_price}}, skip = skip, limit = limit).sort('price').to_list(limit)
    return books

###
# Create an aggregate view of the number of books in the bookstore
# # This is achieived using a 1-stage aggregation query
# 1) GROUP : Taking sum of the stock of each book in the book store
# If the aggregation fails to get a book count, the API responds with 0 count
###
@app.get("/reports/total_books", response_description="Total number of books in the bookstore", response_model=dict)
async def total_books_report():
    pipeline = [
                 {"$group": {"_id": "", "book_count": {"$sum" : "$stock"}}}
               ]
    async for doc in collection.aggregate(pipeline):
        if(book_count := doc.get('book_count', None)) is not None:
            return {'BookCount' : book_count}
    return {'BookCount' : 0}

###
# Create an aggregate view of the top 5 best selling books in the bookstore
# This is achieived using a 3-stage aggregation query
# 1) MATCH : Ignoring all books with 0 copies sold
# 2) SORT  : Sorting all books in decending order of copies sold
# 3) LIMIT : Limiting the results to only the first 5 books in the sorted list
###
@app.get("/reports/top_5_selling", response_description="The top 5 bestselling books in the bookstore", response_model=list)
async def top_selling_books_report():
    pipeline = [
                 {'$match': {'sold': {'$gt': 0}}}, 
                 {'$sort': {'sold': -1}}, 
                 {'$limit': 5},
                 {'$project': {'_id': 0,'Title': '$title','Author': '$author', 'CopiesSold': '$sold'}}
               ]
    return await collection.aggregate(pipeline).to_list(5)

###
# Create an aggregate view of the top 5 authors with the most number of books in the bookstore
# This is achieived using a 5-stage aggregation query
# 1) GROUP  : Finding the sum of copies each book the author has in the store. One author can have multiple books
# 2) MATCH : Ignoring all books with 0 copies in stock
# 3) SORT : Sorting authors in decending order of the sum of copies in stock
# 4) LIMIT  : Limiting the results to only the first 5 authors in the sorted list
# 5) PROJECT : Suppressing the _id field and remapping field names to better match the response
###
@app.get("/reports/top_5_stock_authors", response_description="The top 5 authors with the most number of book copies in the bookstore", response_model=list)
async def top_books_authors_report():
    pipeline = [
                 {'$group': {'_id': '$author','author_copies': {'$sum': '$stock'}}}, 
                 {'$match': {'author_copies': {'$gt': 0}}}, 
                 {'$sort': {'author_copies': -1}}, 
                 {'$limit': 5}, 
                 {'$project': {'_id': 0,'Author': '$_id','Books': '$author_copies'}}
               ]
    return await collection.aggregate(pipeline).to_list(5)

###
# Add new stock of existing book id into the bookstore
# The new stock is added to the existing stock
###
@app.put("/{id}/addStock", response_description="Update stock", response_model=BookModel)
async def update_book_stock(id: str, qty: Annotated[int, Query(gt=0)]):
    update_result = await collection.update_one({"_id": id}, {"$inc": {'stock' : qty}})
    if update_result.modified_count == 1:
        if(updated_book := await collection.find_one({"_id": id})) is not None:
            return updated_book

    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Book {id} not found")

###
# Register sale of existing book id from the bookstore
# While registering a sale, the sold quantity is removed from the stock as well 
# Also checking if someone is trying to sell a book with insufficient stock
###
@app.put("/{id}/addSale", response_description="Update sale", response_model=BookModel)
async def update_book_sale(id: str, qty: Annotated[int, Query(gt=0)]):

    if(book := await collection.find_one({"_id": id})) is not None:
        if  book.get('stock', 0) < qty:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=f"Insufficient stock to make a purchase of {qty} books")
    else:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Book {id} not found")
            
    update_result = await collection.update_one({"_id": id}, {"$inc": {'sold' : qty, 'stock' : -1*qty}})
    return await collection.find_one({"_id": id})
