using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Core.Bulk;
using Elastic.Clients.Elasticsearch.QueryDsl;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddCors(options =>
{
    options.AddPolicy("AllowLocalhost",
        policy =>
        {
            policy.WithOrigins("http://localhost:5183")
                  .AllowAnyHeader()
                  .AllowAnyMethod();
        });
});

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var elasticUrl = builder.Configuration["ElasticSettings:Url"];
var elasticDefaultIndex = builder.Configuration["ElasticSettings:DefaultIndex"];

var settings = new ElasticsearchClientSettings(new Uri(elasticUrl));      
settings.DefaultIndex(elasticDefaultIndex);

ElasticsearchClient client = new(settings);

//Important for if there is no Index it returns error on other operations so it creates initial index
client.IndexAsync(elasticDefaultIndex).GetAwaiter().GetResult();

var app = builder.Build();
app.UseRouting();
app.UseCors("AllowLocalhost");
// Configure the HTTP request pipeline
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.MapPost("/products/create", async (ProductDto request, CancellationToken cancellationToken) =>
{
    Product product = new()
    {   
        ProductId = request.Id,
        Id = request.ElasticId,
        ErpCode = request.ErpCode ?? string.Empty,
        Title = request.Title ?? string.Empty,
        Description = request.Description ?? string.Empty,
        Stock = request.Stock,
        Price = request.Price,
        ImageUrl = request.ImageUrl ?? string.Empty,
        Slug = request.Slug ?? string.Empty,
        Category = request.Category,
        Brand = request.Brand,
        Manufacturer = request.Manufacturer
    };
    CreateRequest<Product> createRequest = new(product.Id.ToString())
    {
        Document = product,
    };

    CreateResponse createResponse = await client.CreateAsync(createRequest, cancellationToken);

    return Results.Ok(createResponse.Id);
});

app.MapPut("/products/update", async (ProductDto request, CancellationToken cancellationToken) =>
{
    Product product = new()
    {
        ProductId = request.Id,
        Id = request.ElasticId,
        ErpCode = request.ErpCode ?? string.Empty,
        Title = request.Title ?? string.Empty,
        Description = request.Description ?? string.Empty,
        Stock = request.Stock,
        Price = request.Price,
        ImageUrl = request.ImageUrl ?? string.Empty,
        Slug = request.Slug ?? string.Empty,
        Category = request.Category,
        Brand = request.Brand,
        Manufacturer = request.Manufacturer
    };
    UpdateRequest<Product, Product> updateRequest = new("products", product.Id.ToString())
    {
        Doc = product,
    };

    UpdateResponse<Product> updateResponse = await client.UpdateAsync(updateRequest, cancellationToken);

    return Results.Ok(new { message = "Update is successful" });
});

app.MapDelete("/products/deleteById", async (Guid id, CancellationToken cancellationToken) =>
{
    DeleteResponse deleteResponse = await client.DeleteAsync("products", id, cancellationToken);

    return Results.Ok(new { message = "Delete is successful" });
});

app.MapPost("/products/getall", async (int size,int page,CancellationToken cancellationToken, string? filter) =>
{
    var fieldsToSearch = new[]
    {
        "erpCode",
        "title",
        "description",
        "imageUrl",
        "slug",
        "category.name",
        "category.description",
        "category.slug",
        "brand.name",
        "brand.description",
        "brand.slug",
        "manufacturer.name",
        "manufacturer.contactInfo",
        "manufacturer.address",
        "manufacturer.slug"
    };

    var query = new BoolQuery();

    if (!string.IsNullOrEmpty(filter))
    {
        query.Should = new Query[]
        {
            new MultiMatchQuery
            {
                Query = filter,
                Fields = fieldsToSearch,
                Type = TextQueryType.BestFields,
                Operator = Operator.Or,
                Fuzziness = new Fuzziness(2)
            },
            new WildcardQuery(new Field("manufacturer.name"))
            {
                Value = "*"+filter+"*"
            },
            new WildcardQuery(new Field("title"))
            {
                Value = "*"+filter+"*"
            },
            new WildcardQuery(new Field("description"))
            {
                Value = "*"+filter+"*"
            },
            new WildcardQuery(new Field("brand.name"))
            {
                Value = "*"+filter+"*"
            }
        };

        query.MinimumShouldMatch = 1; // Minimum bir eşleşme şartı
    }

    // Arama sorgusu oluşturuluyor
    var searchRequest = new SearchRequest
    {
        Size = size,
        From = (page - 1) * size,
        Sort = new List<SortOptions>
        {
            SortOptions.Field(new Field("title.keyword"), new FieldSort { Order = SortOrder.Asc })
        },
        Query = string.IsNullOrEmpty(filter) ? new MatchAllQuery() : query
    };

    SearchResponse<Product> response = await client.SearchAsync<Product>(searchRequest, cancellationToken);
    return Results.Ok(response.Documents);
});

app.MapPost("/products/bulkAdd", async (IList<ProductDto> request, CancellationToken cancellationToken) =>
{
    var bulkRequest = new BulkRequest("products")
    {
        Operations = new List<IBulkOperation>() // Start Operations 
    };

    foreach (var productDto in request)
    {
        Product product = new()
        {
            Id = productDto.ElasticId,
            ProductId = productDto.Id,
            ErpCode = productDto.ErpCode ?? string.Empty,
            Title = productDto.Title ?? string.Empty,
            Description = productDto.Description ?? string.Empty,
            Stock = productDto.Stock,
            Price = productDto.Price,
            ImageUrl = productDto.ImageUrl ?? string.Empty,
            Slug = productDto.Slug ?? string.Empty,
            Category = productDto.Category,
            Brand = productDto.Brand,       
            Manufacturer = productDto.Manufacturer 
        };

        // Add an index operation to the bulk request for each product
        bulkRequest.Operations.Add(new BulkIndexOperation<Product>(product));
    }

    var bulkResponse = await client.BulkAsync(bulkRequest, cancellationToken);

    if (bulkResponse.Errors)
    {
        // If there are errors, return details
        var errorItems = bulkResponse.ItemsWithErrors;
        return Results.BadRequest(new
        {
            message = "Some items failed to be indexed",
            errors = errorItems
        });
    }
    if (bulkResponse == null || bulkResponse.Items == null)
    {
        return Results.Ok(new { message = "Bulk Add is might be successful but there are no items to index"});
    }

    return Results.Ok(new { message = "Bulk Add is successful", indexedCount = bulkResponse.Items.Count });
});

app.Run();

public class Product
{
    public Guid Id { get; set; }
    public int ProductId { get; set; }
    public string ErpCode { get; set; } = string.Empty;
    public string Title { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public int Stock { get; set; }
    public decimal Price { get; set; }
    public string ImageUrl { get; set; } = string.Empty;
    public string Slug { get; set; } = string.Empty;
    public bool IsDeleted { get; set; } 
    public DateTime CreatedDate { get; set; }
    public int CreatedBy { get; set; }
    public Category Category { get; set; }
    public Brand Brand { get; set; }
    public Manufacturer Manufacturer { get; set; }
}
public class ProductDto
{
    public int Id { get; set; }
    public Guid ElasticId { get; set; }
    public string ErpCode { get; set; } = string.Empty;
    public string Title { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public int Stock { get; set; }
    public decimal Price { get; set; }
    public string ImageUrl { get; set; } = string.Empty;
    public string Slug { get; set; } = string.Empty;
    public bool IsDeleted { get; set; }
    public DateTime CreatedDate { get; set; } 
    public int CreatedBy { get; set; }
    public Category? Category { get; set; }
    public Brand? Brand { get; set; }
    public Manufacturer? Manufacturer { get; set; }
}
public class Category
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public string Slug { get; set; } = string.Empty;
    public bool IsDeleted { get; set; } 
    public DateTime CreatedDate { get; set; } 
    public int CreatedBy { get; set; }
}
public class Brand
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public string Slug { get; set; } = string.Empty;
    public bool IsDeleted { get; set; }
    public DateTime CreatedDate { get; set; }
    public int CreatedBy { get; set; }
}
public class Manufacturer
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string ContactInfo { get; set; } = string.Empty;
    public string Address { get; set; } = string.Empty;
    public string Slug { get; set; } = string.Empty;
    public bool IsDeleted { get; set; }
    public DateTime CreatedDate { get; set; }
    public int CreatedBy { get; set; }
}