//本代码有由AI生成
use mongodb::{
    bson::{doc, Document, Bson},
    Collection, Database,
    error::Result,
    options::{UpdateOptions, FindOptions, AggregateOptions},
};
use futures::TryStreamExt;
use serde::{de::DeserializeOwned, Serialize};

#[derive(Debug, Clone)]
enum PipelineStage {
    Match(Document),
    Group(Document),
    Sort(Document),
    Limit(i64),
    Skip(i64),
    Project(Document),
    Lookup(LookupStage),
    Unwind(String),
    Count(String),
    AddFields(Document),
    ReplaceRoot(Document),
    Facet(Document),
    Bucket(Document),
    SortByCount(Bson),
    GraphLookup(Document),
    Out(String),
}

#[derive(Debug, Clone)]
struct LookupStage {
    from: String,
    local_field: String,
    foreign_field: String,
    as_field: String,
    pipeline: Option<Vec<Document>>,
}

#[derive(Debug)]
enum UpdateOperation {
    Set(Document),
    Unset(Document),
    Pull(Document),
    Push(Document),
    AddToSet(Document),
    Inc(Document),
    Max(Document),
    Min(Document),
    Mul(Document),
    Rename(Document),
    CurrentDate(Document),
}

pub struct MongoAggregateBuilder<'a, T> 
where 
    T: Serialize + DeserializeOwned + Unpin + Send + Sync + 'static
{
    collection: Collection<T>,
    database: &'a Database,
    pipeline_stages: Vec<PipelineStage>,
    update_operations: Vec<UpdateOperation>,
    filter: Option<Document>,
    options: Option<UpdateOptions>,
    find_options: Option<FindOptions>,
    aggregate_options: Option<AggregateOptions>,
    is_upsert: bool,
}

impl<'a, T> MongoAggregateBuilder<'a, T> 
where 
    T: Serialize + DeserializeOwned + Unpin + Send + Sync
{
    pub fn new(collection: Collection<T>, database: &'a Database) -> Self {
        Self {
            collection,
            database,
            pipeline_stages: Vec::new(),
            update_operations: Vec::new(),
            filter: None,
            options: None,
            find_options: None,
            aggregate_options: None,
            is_upsert: false,
        }
    }

    // 基础配置方法
    /*
        关键区别：
        $facet 方式：并行执行多个独立查询，结果分开返回
        多 $match 方式：串行执行多个查询条件，结果合并返回
        选择建议：
        需要完全不同的查询结果时，使用 $facet
        需要在同一数据集上应用多个条件时，使用多个 $match
     */
    /*  
        filter 配合match 实现多查询（串行）
        builder
        .filter(doc! { "type": "order" })  // 第一个查询条件
        .lookup("users", "user_id", "_id", "user_info")  // 关联查询
        .match(doc! { "status": "active" })  // 第二个查询条件
        .project(doc! { "id": 1, "user_info": 1 })
        .execute_aggregate()
    
     */
    pub fn filter(mut self, filter: Document) -> Self {
        self.filter = Some(filter);
        self
    }

    pub fn upsert(mut self, upsert: bool) -> Self {
        self.is_upsert = upsert;
        self
    }

    // 聚合管道构建方法
    pub fn lookup(mut self, from: impl Into<String>, local_field: impl Into<String>, 
                 foreign_field: impl Into<String>, as_field: impl Into<String>) -> Self {
        self.pipeline_stages.push(PipelineStage::Lookup(LookupStage {
            from: from.into(),
            local_field: local_field.into(),
            foreign_field: foreign_field.into(),
            as_field: as_field.into(),
            pipeline: None,
        }));
        self
    }

    pub fn lookup_with_pipeline(mut self, from: impl Into<String>, local_field: impl Into<String>,
                              foreign_field: impl Into<String>, as_field: impl Into<String>,
                              pipeline: Vec<Document>) -> Self {
        self.pipeline_stages.push(PipelineStage::Lookup(LookupStage {
            from: from.into(),
            local_field: local_field.into(),
            foreign_field: foreign_field.into(),
            as_field: as_field.into(),
            pipeline: Some(pipeline),
        }));
        self
    }

    pub fn unwind(mut self, path: impl Into<String>) -> Self {
        self.pipeline_stages.push(PipelineStage::Unwind(path.into()));
        self
    }

    pub fn group(mut self, group: Document) -> Self {
        self.pipeline_stages.push(PipelineStage::Group(group));
        self
    }

    pub fn sort(mut self, sort: Document) -> Self {
        self.pipeline_stages.push(PipelineStage::Sort(sort));
        self
    }

    pub fn project(mut self, project: Document) -> Self {
        self.pipeline_stages.push(PipelineStage::Project(project));
        self
    }

    pub fn add_fields(mut self, fields: Document) -> Self {
        self.pipeline_stages.push(PipelineStage::AddFields(fields));
        self
    }

    pub fn replace_root(mut self, new_root: Document) -> Self {
        self.pipeline_stages.push(PipelineStage::ReplaceRoot(new_root));
        self
    }

    /*
        这个操作可以实现多个查询：
        let results = MongoAggregateBuilder::new(collection, database)
                                                .facet(doc! {
                                                    "query1": [
                                                        { "$match": { "id": 1 } },
                                                        { "$project": { "name": 1, "count": 1 } }
                                                    ],
                                                    "query2": [
                                                        { "$match": { "status": "active" } },
                                                        { "$sort": { "created_at": -1 } }
                                                    ],
                                                    "query3": [
                                                        { "$match": { "type": "special" } },
                                                        { "$group": { 
                                                            "_id": "$category",
                                                            "total": { "$sum": 1 }
                                                        }}
                                                    ]
                                                })
                                                .execute_aggregate()
                                                .await?;
    
     */
    pub fn facet(mut self, facet: Document) -> Self {
        self.pipeline_stages.push(PipelineStage::Facet(facet));
        self
    }

    // 更新操作构建方法
    pub fn set(mut self, update: Document) -> Self {
        self.update_operations.push(UpdateOperation::Set(update));
        self
    }

    pub fn unset(mut self, fields: Document) -> Self {
        self.update_operations.push(UpdateOperation::Unset(fields));
        self
    }

    pub fn pull(mut self, pull: Document) -> Self {
        self.update_operations.push(UpdateOperation::Pull(pull));
        self
    }

    pub fn push(mut self, push: Document) -> Self {
        self.update_operations.push(UpdateOperation::Push(push));
        self
    }

    pub fn add_to_set(mut self, add: Document) -> Self {
        self.update_operations.push(UpdateOperation::AddToSet(add));
        self
    }

    pub fn inc(mut self, inc: Document) -> Self {
        self.update_operations.push(UpdateOperation::Inc(inc));
        self
    }

    // 构建更新文档
    fn build_update_doc(&self) -> Document {
        let mut update = Document::new();
        
        for op in &self.update_operations {
            match op {
                UpdateOperation::Set(doc) => { update.insert("$set", doc); }
                UpdateOperation::Unset(doc) => { update.insert("$unset", doc); }
                UpdateOperation::Pull(doc) => { update.insert("$pull", doc); }
                UpdateOperation::Push(doc) => { update.insert("$push", doc); }
                UpdateOperation::AddToSet(doc) => { update.insert("$addToSet", doc); }
                UpdateOperation::Inc(doc) => { update.insert("$inc", doc); }
                UpdateOperation::Max(doc) => { update.insert("$max", doc); }
                UpdateOperation::Min(doc) => { update.insert("$min", doc); }
                UpdateOperation::Mul(doc) => { update.insert("$mul", doc); }
                UpdateOperation::Rename(doc) => { update.insert("$rename", doc); }
                UpdateOperation::CurrentDate(doc) => { update.insert("$currentDate", doc); }
            }
        }
        update
    }

    // 构建聚合管道
    fn build_pipeline(&self) -> Vec<Document> {
        let mut pipeline = Vec::new();
        
        if let Some(filter) = &self.filter {
            pipeline.push(doc! { "$match": filter });
        }

        for ref stage in &self.pipeline_stages {
            match stage {
                PipelineStage::Match(doc) => { 
                    pipeline.push(doc! { "$match": doc }); 
                }
                PipelineStage::Group(doc) => { 
                    pipeline.push(doc! { "$group": doc }); 
                }
                PipelineStage::Sort(doc) => { 
                    pipeline.push(doc! { "$sort": doc }); 
                }
                PipelineStage::Limit(limit) => { 
                    pipeline.push(doc! { "$limit": limit }); 
                }
                PipelineStage::Skip(skip) => {
                    pipeline.push(doc! { "$skip": skip });
                }
                PipelineStage::Project(doc) => { 
                    pipeline.push(doc! { "$project": doc }); 
                }
                PipelineStage::Lookup(lookup) => {
                    let mut lookup_doc = doc! {
                        "from": &lookup.from,
                        "localField": &lookup.local_field,
                        "foreignField": &lookup.foreign_field,
                        "as": &lookup.as_field
                    };
                    if let Some(pipeline) = &lookup.pipeline {
                        lookup_doc.insert("pipeline", pipeline);
                    }
                    pipeline.push(doc! { "$lookup": lookup_doc });
                }
                PipelineStage::Unwind(path) => {
                    pipeline.push(doc! { "$unwind": format!("${}", path) });
                }
                PipelineStage::AddFields(doc) => {
                    pipeline.push(doc! { "$addFields": doc });
                }
                PipelineStage::ReplaceRoot(doc) => {
                    pipeline.push(doc! { "$replaceRoot": doc });
                }
                PipelineStage::Facet(doc) => {
                    pipeline.push(doc! { "$facet": doc });
                }
                PipelineStage::Count(field) => {
                    pipeline.push(doc! { "$count": field });
                }
                PipelineStage::Bucket(doc) => {
                    pipeline.push(doc! { "$bucket": doc });
                }
                PipelineStage::SortByCount(expression) => {
                    pipeline.push(doc! { "$sortByCount": expression });
                }
                PipelineStage::GraphLookup(doc) => {
                    pipeline.push(doc! { "$graphLookup": doc });
                }
                PipelineStage::Out(collection) => {
                    pipeline.push(doc! { "$out": collection });
                }
            }
        }
        pipeline
    }

    // 执行方法
    pub async fn execute_aggregate<U: DeserializeOwned>(&self) -> Result<Vec<U>> {
        let pipeline = self.build_pipeline();
        let mut cursor = self.collection.aggregate(pipeline).await?;
        
        let mut results = Vec::new();
        while let Some(result) = cursor.try_next().await? {
            results.push(mongodb::bson::from_document(result)?);
        }
        Ok(results)
    }

    pub async fn execute_update(&self) -> Result<mongodb::results::UpdateResult> {
        if let Some(filter) = &self.filter {
            let update = self.build_update_doc();
            let _ = if self.is_upsert {
                Some(UpdateOptions::builder().upsert(true).build())
            } else {
                None
            };
            Ok(self.collection.update_one(filter.clone(), update).await?)
        } else {
            Err(mongodb::error::Error::custom("No filter specified"))
        }
    }

    pub async fn execute_update_many(&self) -> Result<mongodb::results::UpdateResult> {
        if let Some(filter) = &self.filter {
            let update = self.build_update_doc();
            Ok(self.collection.update_many(filter.clone(), update).await?)
        } else {
            Err(mongodb::error::Error::custom("No filter specified"))
        }
    }
}

