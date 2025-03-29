//! MongoDB批量写入构建器
//! 
//! 这个模块提供了一个流式API来构建和执行MongoDB的批量写入操作。
//! 支持以下操作:
//! 
//! - 更新操作(UpdateOne):
//!   - set: 设置字段值
//!   - unset: 删除字段
//!   - setOnInsert: 仅在插入时设置字段值
//!   - push/pushEach: 向数组添加元素
//!   - pull/pullAll: 从数组移除元素
//!   - pop: 从数组头部或尾部移除元素
//!   - addToSet/addToSetEach: 向数组添加不重复元素
//!   - inc: 增加数值
//!   - mul: 乘以数值
//!   - min/max: 设置最小/最大值
//!   - rename: 重命名字段
//!   - currentDate: 设置当前日期
//!   - bit: 位操作
//!
//! - 删除操作:
//!   - deleteOne: 删除单个文档
//!   - deleteMany: 删除多个文档
//!
//! - 插入操作:
//!   - insertOne: 插入单个文档
//!
//! 所有操作都可以链式调用,最后通过execute()执行批量写入。
//! 支持自动合并相同类型的更新操作以提高性能。

use mongodb::{
    bson::{Document, to_document, Bson,doc},
    Collection,
    options::{WriteModel, UpdateOneModel, DeleteOneModel, 
        DeleteManyModel, InsertOneModel, UpdateModifications},
    action::BulkWrite,
    results::SummaryBulkWriteResult
};
use serde::{Serialize, de::DeserializeOwned};

#[derive(Debug, Clone)]
pub enum WriteOperation<T> where
                            T: Serialize + 
                            DeserializeOwned + 
                            Unpin + Send + Sync + 
                            'static {
    UpdateOne {
        filter: Document,
        updates: Vec<UpdateType>,  // 存储 UpdateType 而不是 Document
        upsert: bool,
    },
    DeleteOne {
        filter: Document,
    },
    DeleteMany {
        filter: Document,
    },
    InsertOne {
        document: T,
    },
}

#[derive(Debug, Clone)]
pub enum UpdateType {
    Set(Document),
    Unset(Document),
    SetOnInsert(Document),
    Push(Document),
    PushEach(Document),
    Pull(Document),
    PullAll(Document),
    Pop(Document),
    AddToSet(Document),
    AddToSetEach(Document),
    Inc(Document),
    Mul(Document),
    Min(Document),
    Max(Document),
    Rename(Document),
    CurrentDate(Document),
    Bit(Document),
}

impl UpdateType {
    fn compare_bson_values(v1: &Bson, v2: &Bson) -> Option<std::cmp::Ordering> {
        match (v1, v2) {
            (Bson::Double(d1), Bson::Double(d2)) => Some(d1.partial_cmp(d2)?),
            (Bson::Int32(i1), Bson::Int32(i2)) => Some(i1.cmp(i2)),
            (Bson::Int64(i1), Bson::Int64(i2)) => Some(i1.cmp(i2)),
            _ => None,
        }
    }

    /*
    对于简单覆盖型操作（Set, Unset, SetOnInsert, Rename, Bit），合并所有字段
    对于数组操作（Push, PushEach, Pull, PullAll, AddToSet, AddToSetEach），正确处理数组元素
    对于数值操作（Inc, Mul），正确处理不同数值类型的计算
    对于比较操作（Min, Max），保持正确的比较语义
    对于特殊操作（Pop, CurrentDate），使用最后一个操作的值
     */
    fn merge(&self, other: &UpdateType) -> Option<UpdateType> {
        match (self, other) {
            // Set: 后面的值覆盖前面的值
            (UpdateType::Set(doc1), UpdateType::Set(doc2)) => {
                let mut merged = doc1.clone();
                for (k, v) in doc2.iter() {
                    merged.insert(k, v.clone());
                }
                Some(UpdateType::Set(merged))
            },
    
            // Unset: 合并所有要取消设置的字段
            (UpdateType::Unset(doc1), UpdateType::Unset(doc2)) => {
                let mut merged = doc1.clone();
                for (k, v) in doc2.iter() {
                    merged.insert(k, v.clone());
                }
                Some(UpdateType::Unset(merged))
            },
    
            // SetOnInsert: 合并所有插入时要设置的字段
            (UpdateType::SetOnInsert(doc1), UpdateType::SetOnInsert(doc2)) => {
                let mut merged = doc1.clone();
                for (k, v) in doc2.iter() {
                    merged.insert(k, v.clone());
                }
                Some(UpdateType::SetOnInsert(merged))
            },
            /*

             MongoDB 的默认行为，即：
            $push 会将整个值作为一个元素添加到数组中，不管这个值是不是数组
            只有使用 $push 配合 $each 才会将数组的元素分别添加
                        
            // 向数组添加单个元素
            db.collection.updateOne(
                { _id: 1 },
                { $push: { tags: "mongodb" } }
            )
            
            // 向数组添加多个元素
            db.collection.updateOne(
                { _id: 1 },
                { 
                $push: {
                    tags: {
                    $each: ["mongodb", "database", "nosql"]
                    }
                }
            )
            
            // 向数组添加多个元素并排序
            db.collection.updateOne(
                { _id: 1 },
                {
                $push: {
                    scores: {
                        $each: [90, 92, 85],       // 要添加的元素
                        $sort: -1,                 // 排序（1 升序，-1 降序）
                        $slice: 3,                 // 只保留前3个元素
                        $position: 0               // 从数组开头插入
                    }
                }
            )
            
            */
            /*
            自动将数组转换为 $each 格式
            保持单个值的简单形式
            符合 MongoDB 的原生行为
            如果需要更复杂的数组操作（比如添加多个值并指定位置、排序等），应该使用 push_each 方法。
            builder.push(doc! { "tags": "mongodb" })
            // 生成: { $push: { tags: "mongodb" } }
            builder.push(doc! { "tags": ["mongodb", "database"] })
            // 生成: { $push: { tags: { $each: ["mongodb", "database"] } } }
             */
            // Push: 合并数组
            (UpdateType::Push(doc1), UpdateType::Push(doc2)) => {
                let mut merged = doc1.clone();
                for (k, v) in doc2.iter() {
                    match v {
                        // 如果是数组，应该转换为 $each 格式
                        Bson::Array(arr) => {
                            let each_doc = doc! { "$each": arr };
                            merged.insert(k, Bson::Document(each_doc));
                        },
                        // 如果是单个值，直接使用
                        _ => {
                            merged.insert(k, v.clone());
                        }
                    }
                }
                Some(UpdateType::Push(merged))
            },
           
            /*

                2.$push with $each：
                push_each(doc! { "tags": { "$each": ["mongodb", "nosql"] } })
                这会将数组中的每个元素分别添加到目标数组中。
                结果会是：["existing_tags", "mongodb", "nosql"]
                另外，pushEach 还支持更多的修饰符：
                push_each(doc! { 
                    "tags": {
                        "$each": ["mongodb", "nosql"],
                        "$position": 0,        // 插入位置
                        "$slice": 5,          // 限制数组大小
                        "$sort": 1            // 排序
                    }
                })             
             */
             // PushEach: 合并 $each 操作

             /*
                // 这些调用都会正确工作
                builder.push_each(doc! { "tags": ["mongodb", "nosql"] })  // 自动转换为 $each 格式
                builder.push_each(doc! { "tags": "mongodb" })  // 自动转换为 $each 格式
                builder.push_each(doc! { "tags": { "$each": ["mongodb"], "$position": 0 } })  // 保持原有格式和修饰符
             */
             (UpdateType::PushEach(doc1), UpdateType::PushEach(doc2)) => {
                let mut merged = doc1.clone();
                for (k, v) in doc2.iter() {
                    let new_doc = match v {
                        // 如果已经是正确的格式（包含 $each），保持原样
                        Bson::Document(d) if d.contains_key("$each") => d.clone(),
                        // 如果是数组，转换为 $each 格式
                        Bson::Array(arr) => doc! { "$each": arr },
                        // 如果是单个值，转换为包含单个元素的 $each 数组
                        other => doc! { "$each": [other.clone()] },
                    };
            
                    if let Some(Bson::Document(existing)) = merged.get(k).map(|v| v.clone()) {
                        let mut combined = existing.clone();
                        // 合并 $each 数组
                        let existing_arr = existing.get("$each")
                            .and_then(|v| v.as_array())
                            .unwrap_or(&Vec::new())
                            .clone();
                        let new_arr = new_doc.get("$each")
                            .and_then(|v| v.as_array())
                            .unwrap_or(&Vec::new())
                            .clone();
                        
                        let mut merged_arr = existing_arr;
                        merged_arr.extend(new_arr);
                        combined.insert("$each", Bson::Array(merged_arr));
                        
                        // 保留其他修饰符
                        for (mod_k, mod_v) in existing.iter() {
                            if mod_k != "$each" {
                                combined.insert(mod_k, mod_v.clone());
                            }
                        }
                        // 合并新的修饰符
                        for (mod_k, mod_v) in new_doc.iter() {
                            if mod_k != "$each" {
                                combined.insert(mod_k, mod_v.clone());
                            }
                        }
                        
                        merged.insert(k, Bson::Document(combined));
                    } else {
                        merged.insert(k, Bson::Document(new_doc));
                    }
                }
                Some(UpdateType::PushEach(merged))
            },
    
            // Pull: 使用最新的条件
            /*
            builder.add_update(filter)
            .pull(doc! { "scores": { "$gt": 5 } })
            .pull(doc! { "fruits": "apple" })
             */
            (UpdateType::Pull(doc1), UpdateType::Pull(doc2)) => {
                let mut merged = doc1.clone();
                for (k, v) in doc2.iter() {
                    merged.insert(k, v.clone());  // 使用最新的条件
                }
                Some(UpdateType::Pull(merged))
            },
    
            // PullAll: 合并要移除的元素数组
            /*
                builder.pull_all(doc! { "fruits": ["apple", "banana"] })
             */
            (UpdateType::PullAll(doc1), UpdateType::PullAll(doc2)) => {
                let mut merged = doc1.clone();
                for (k, v) in doc2.iter() {
                    if let (Some(Bson::Array(existing)), Some(new_values)) = (merged.get(k).map(|v| v.clone()), v.as_array()) {
                        let mut combined = existing.clone();
                        combined.extend(new_values.iter().cloned());
                        merged.insert(k, Bson::Array(combined));
                    } else {
                        merged.insert(k, v.clone());
                    }
                }
                Some(UpdateType::PullAll(merged))
            },
    
            // Pop: 不合并，使用最后一个操作
            (UpdateType::Pop(_), UpdateType::Pop(doc2)) => {
                Some(UpdateType::Pop(doc2.clone()))
            },
    
            // AddToSet: 将值作为单个元素添加，保持唯一性
            /*
            自动将数组转换为 $each 格式
            保持单个值的简单形式
            符合 MongoDB 的原生行为
            如果需要更复杂的数组操作（比如添加多个值并保持唯一性），应该使用 add_to_set_each 方法。
            builder.add_to_set(doc! { "tags": "mongodb" })
            // 生成: { $addToSet: { tags: "mongodb" } }

            builder.add_to_set(doc! { "tags": ["mongodb", "database"] })
            // 生成: { $addToSet: { tags: { $each: ["mongodb", "database"] } } }

             */
            (UpdateType::AddToSet(doc1), UpdateType::AddToSet(doc2)) => {
                let mut merged = doc1.clone();
                for (k, v) in doc2.iter() {
                    match v {
                        // 如果是数组，应该转换为 $each 格式
                        Bson::Array(arr) => {
                            let each_doc = doc! { "$each": arr };
                            merged.insert(k, Bson::Document(each_doc));
                        },
                        // 如果是单个值，直接使用
                        _ => {
                            merged.insert(k, v.clone());
                        }
                    }
                }
                Some(UpdateType::AddToSet(merged))
            },

                
            // AddToSetEach: 合并 $each 操作并保持唯一性
            /*
            // 添加单个值
            builder.add_to_set_each(doc! { "tags": "mongodb" })
            // 生成: { $addToSet: { tags: { $each: ["mongodb"] } } }

            // 添加数组
            builder.add_to_set_each(doc! { "tags": ["mongodb", "database"] })
            // 生成: { $addToSet: { tags: { $each: ["mongodb", "database"] } } }

            // 使用完整格式
            builder.add_to_set_each(doc! { 
                "tags": { 
                    "$each": ["mongodb", "database"],
                    "$position": 0  // 其他修饰符会被保留
                } 
            })

            // 多次调用会正确合并并保持唯一性
            builder.add_to_set_each(doc! { "tags": ["mongodb"] })
                .add_to_set_each(doc! { "tags": ["database", "mongodb"] })
            // 最终结果: { $addToSet: { tags: { $each: ["mongodb", "database"] } } }
             */
            (UpdateType::AddToSetEach(doc1), UpdateType::AddToSetEach(doc2)) => {
                let mut merged = doc1.clone();
                for (k, v) in doc2.iter() {
                    let new_doc = match v {
                        // 如果已经是正确的格式（包含 $each），保持原样
                        Bson::Document(d) if d.contains_key("$each") => d.clone(),
                        // 如果是数组，转换为 $each 格式
                        Bson::Array(arr) => doc! { "$each": arr },
                        // 如果是单个值，转换为包含单个元素的 $each 数组
                        other => doc! { "$each": [other.clone()] },
                    };

                    if let Some(Bson::Document(existing)) = merged.get(k).map(|v| v.clone()) {
                        let mut combined = existing.clone();
                        // 合并 $each 数组，保持唯一性
                        let existing_arr = existing.get("$each")
                            .and_then(|v| v.as_array())
                            .unwrap_or(&Vec::new())
                            .clone();
                        let new_arr = new_doc.get("$each")
                            .and_then(|v| v.as_array())
                            .unwrap_or(&Vec::new())
                            .clone();
                        
                        let mut merged_arr = existing_arr;
                        // 保持唯一性
                        for new_v in new_arr {
                            if !merged_arr.contains(&new_v) {
                                merged_arr.push(new_v.clone());
                            }
                        }
                        combined.insert("$each", Bson::Array(merged_arr));
                        
                        // 保留其他修饰符
                        for (mod_k, mod_v) in existing.iter() {
                            if mod_k != "$each" {
                                combined.insert(mod_k, mod_v.clone());
                            }
                        }
                        // 合并新的修饰符
                        for (mod_k, mod_v) in new_doc.iter() {
                            if mod_k != "$each" {
                                combined.insert(mod_k, mod_v.clone());
                            }
                        }
                        
                        merged.insert(k, Bson::Document(combined));
                    } else {
                        merged.insert(k, Bson::Document(new_doc));
                    }
                }
                Some(UpdateType::AddToSetEach(merged))
            },
                
            // Inc: 数值相加
            (UpdateType::Inc(doc1), UpdateType::Inc(doc2)) => {
                let mut merged = doc1.clone();
                for (k, v) in doc2.iter() {
                    if let (Some(Bson::Double(existing)), Some(new_value)) = (merged.get(k).map(|v| v.clone()), v.as_f64()) {
                        merged.insert(k, Bson::Double(existing + new_value));
                    } else if let (Some(Bson::Int32(existing)), Some(new_value)) = (merged.get(k).map(|v| v.clone()), v.as_i32()) {
                        merged.insert(k, Bson::Int32(existing + new_value));
                    } else if let (Some(Bson::Int64(existing)), Some(new_value)) = (merged.get(k).map(|v| v.clone()), v.as_i64()) {
                        merged.insert(k, Bson::Int64(existing + new_value));
                    } else {
                        merged.insert(k, v.clone());
                    }
                }
                Some(UpdateType::Inc(merged))
            },
    
            // Mul: 数值相乘
            (UpdateType::Mul(doc1), UpdateType::Mul(doc2)) => {
                let mut merged = doc1.clone();
                for (k, v) in doc2.iter() {
                    if let (Some(Bson::Double(existing)), Some(new_value)) = (merged.get(k).map(|v| v.clone()), v.as_f64()) {
                        merged.insert(k, Bson::Double(existing * new_value));
                    } else if let (Some(Bson::Int32(existing)), Some(new_value)) = (merged.get(k).map(|v| v.clone()), v.as_i32()) {
                        merged.insert(k, Bson::Int32(existing * new_value));
                    } else if let (Some(Bson::Int64(existing)), Some(new_value)) = (merged.get(k).map(|v| v.clone()), v.as_i64()) {
                        merged.insert(k, Bson::Int64(existing * new_value));
                    } else {
                        merged.insert(k, v.clone());
                    }
                }
                Some(UpdateType::Mul(merged))
            },
    
            // Min: 取较小值
            (UpdateType::Min(doc1), UpdateType::Min(doc2)) => {
                let mut merged = doc1.clone();
                for (k, v) in doc2.iter() {
                    if let Some(existing) = merged.get(k) {
                        if let Some(ordering) = Self::compare_bson_values(v, existing) {
                            if ordering == std::cmp::Ordering::Less {
                                merged.insert(k, v.clone());
                            }
                        }
                    } else {
                        merged.insert(k, v.clone());
                    }
                }
                Some(UpdateType::Min(merged))
            },
    
            // Max: 取较大值
            (UpdateType::Max(doc1), UpdateType::Max(doc2)) => {
                let mut merged = doc1.clone();
                for (k, v) in doc2.iter() {
                    if let Some(existing) = merged.get(k) {
                        if let Some(ordering) = Self::compare_bson_values(v, existing) {
                            if ordering == std::cmp::Ordering::Greater {
                                merged.insert(k, v.clone());
                            }
                        }
                    } else {
                        merged.insert(k, v.clone());
                    }
                }
                Some(UpdateType::Max(merged))
            },
    
            // Rename: 合并重命名操作
            (UpdateType::Rename(doc1), UpdateType::Rename(doc2)) => {
                let mut merged = doc1.clone();
                for (k, v) in doc2.iter() {
                    merged.insert(k, v.clone());
                }
                Some(UpdateType::Rename(merged))
            },
    
            // CurrentDate: 使用最后一个操作
            (UpdateType::CurrentDate(_), UpdateType::CurrentDate(doc2)) => {
                Some(UpdateType::CurrentDate(doc2.clone()))
            },
    
            // Bit: 按位操作合并
            (UpdateType::Bit(doc1), UpdateType::Bit(doc2)) => {
                let mut merged = doc1.clone();
                for (k, v) in doc2.iter() {
                    merged.insert(k, v.clone());
                }
                Some(UpdateType::Bit(merged))
            },
    
            // 不同类型的操作不能合并
            _ => None
        }
    }
    fn to_document(&self) -> Document {
        match self {
            UpdateType::Set(doc) => doc! { "$set": doc },
            UpdateType::Unset(doc) => doc! { "$unset": doc },
            UpdateType::SetOnInsert(doc) => doc! { "$setOnInsert": doc },
            UpdateType::Push(doc) => doc! { "$push": doc },
            UpdateType::PushEach(doc) => doc! { "$push": doc },
            UpdateType::Pull(doc) => doc! { "$pull": doc },
            UpdateType::PullAll(doc) => doc! { "$pullAll": doc },
            UpdateType::Pop(doc) => doc! { "$pop": doc },
            UpdateType::AddToSet(doc) => doc! { "$addToSet": doc },
            UpdateType::AddToSetEach(doc) => doc! { "$addToSet": doc },
            UpdateType::Inc(doc) => doc! { "$inc": doc },
            UpdateType::Mul(doc) => doc! { "$mul": doc },
            UpdateType::Min(doc) => doc! { "$min": doc },
            UpdateType::Max(doc) => doc! { "$max": doc },
            UpdateType::Rename(doc) => doc! { "$rename": doc },
            UpdateType::CurrentDate(doc) => doc! { "$currentDate": doc },
            UpdateType::Bit(doc) => doc! { "$bit": doc },
        }
    }
}

struct BatchUpdateContext{
    filter: Document,
    updates: Vec<UpdateType>,
    is_upsert: bool,
}

pub struct BatchUpdateBuilder<T>
where
    T: Serialize + DeserializeOwned + Unpin + Send + Sync + 'static,
{
    operations: Vec<WriteOperation<T>>,
    ordered: bool,
    current_context: Option<BatchUpdateContext>,
    collection: Collection<T>,
}

impl<T> BatchUpdateBuilder<T>
where
    T: Serialize + DeserializeOwned + Unpin + Send + Sync + 'static,
{
    pub fn new(collection: Collection<T>) -> Self {
        Self {
            operations: Vec::new(),
            ordered: true,
            current_context: None,
            collection: collection,
        }
    }
    

    pub fn ordered(mut self, ordered: bool) -> Self {
        self.ordered = ordered;
        self
    }

    fn commit_context(&mut self) {
        if let Some(context) = self.current_context.take() {
            if !context.updates.is_empty() {
                self.operations.push(WriteOperation::UpdateOne {
                    filter: context.filter,
                    updates: context.updates,
                    upsert: context.is_upsert,
                });
            }
        }
    }



    fn documents_equal(doc1: &Document, doc2: &Document) -> bool {
        if doc1.len() != doc2.len() {
            return false;
        }
        
        for (key, value1) in doc1.iter() {
            match doc2.get(key) {
                Some(value2) if value1 == value2 => continue,
                _ => return false,
            }
        }
        true
    }

    pub fn add_update(&mut self, filter: Document) -> &mut Self {
        // 提交当前上下文
        self.commit_context();

        // 查找是否存在相同filter的操作
        let existing_update = self.operations.iter()
            .position(|op| {
                if let WriteOperation::UpdateOne { filter: existing_filter, .. } = op {
                    Self::documents_equal(existing_filter, &filter)
                } else {
                    false
                }
            })
            .map(|index| self.operations.remove(index));

        // 根据查找结果创建新的上下文
        self.current_context = Some(match existing_update {
            Some(WriteOperation::UpdateOne { updates, upsert, .. }) => BatchUpdateContext {
                filter,
                updates,  // 使用已存在的updates
                is_upsert: upsert,
            },
            _ => BatchUpdateContext {
                filter,
                updates: Vec::new(),
                is_upsert: false,
            }
        });

        self
    }

    pub fn set(&mut self, doc: Document) -> &mut Self {
        if let Some(context) = &mut self.current_context {
            context.add_update_type(UpdateType::Set(doc));
        }
        self
    }

    pub fn unset(&mut self, doc: Document) -> &mut Self {
        if let Some(context) = &mut self.current_context {
            context.add_update_type(UpdateType::Unset(doc));
        }
        self
    }

    pub fn set_on_insert(&mut self, doc: Document) -> &mut Self {
        if let Some(context) = &mut self.current_context {
            context.add_update_type(UpdateType::SetOnInsert(doc));
        }
        self
    }

    pub fn push(&mut self, doc: Document) -> &mut Self {
        if let Some(context) = &mut self.current_context {
            context.add_update_type(UpdateType::Push(doc));
        }
        self
    }

    pub fn push_each(&mut self, doc: Document) -> &mut Self {
        if let Some(context) = &mut self.current_context {
            context.add_update_type(UpdateType::PushEach(doc));
        }
        self
    }

    pub fn pull(&mut self, doc: Document) -> &mut Self {
        if let Some(context) = &mut self.current_context {
            context.add_update_type(UpdateType::Pull(doc));
        }
        self
    }

    pub fn pull_all(&mut self, doc: Document) -> &mut Self {
        if let Some(context) = &mut self.current_context {
            context.add_update_type(UpdateType::PullAll(doc));
        }
        self
    }

    pub fn pop(&mut self, doc: Document) -> &mut Self {
        if let Some(context) = &mut self.current_context {
            context.add_update_type(UpdateType::Pop(doc));
        }
        self
    }

    pub fn add_to_set(&mut self, doc: Document) -> &mut Self {
        if let Some(context) = &mut self.current_context {
            context.add_update_type(UpdateType::AddToSet(doc));
        }
        self
    }

    pub fn add_to_set_each(&mut self, doc: Document) -> &mut Self {
        if let Some(context) = &mut self.current_context {
            context.add_update_type(UpdateType::AddToSetEach(doc));
        }
        self
    }

    pub fn inc(&mut self, doc: Document) -> &mut Self {
        if let Some(context) = &mut self.current_context {
            context.add_update_type(UpdateType::Inc(doc));
        }
        self
    }

    pub fn mul(&mut self, doc: Document) -> &mut Self {
        if let Some(context) = &mut self.current_context {
            context.add_update_type(UpdateType::Mul(doc));
        }
        self
    }

    pub fn min(&mut self, doc: Document) -> &mut Self {
        if let Some(context) = &mut self.current_context {
            context.add_update_type(UpdateType::Min(doc));
        }
        self
    }

    pub fn max(&mut self, doc: Document) -> &mut Self {
        if let Some(context) = &mut self.current_context {
            context.add_update_type(UpdateType::Max(doc));
        }
        self
    }

    pub fn rename(&mut self, doc: Document) -> &mut Self {
        if let Some(context) = &mut self.current_context {
            context.add_update_type(UpdateType::Rename(doc));
        }
        self
    }

    pub fn current_date(&mut self, doc: Document) -> &mut Self {
        if let Some(context) = &mut self.current_context {
            context.add_update_type(UpdateType::CurrentDate(doc));
        }
        self
    }

    pub fn bit(&mut self, doc: Document) -> &mut Self {
        if let Some(context) = &mut self.current_context {
            context.add_update_type(UpdateType::Bit(doc));
        }
        self
    }

    pub fn upsert(&mut self, upsert: bool) -> &mut Self {
        if let Some(context) = &mut self.current_context {
            context.is_upsert = upsert;
        }
        self
    }

    pub fn delete(&mut self, filter: Document) -> &mut Self {
        self.commit_context();
        self.operations.push(WriteOperation::DeleteOne { filter });
        self
    }

    pub fn delete_many(&mut self, filter: Document) -> &mut Self {
        self.commit_context();
        self.operations.push(WriteOperation::DeleteMany { filter });
        self
    }

    pub fn insert(&mut self, document: T) -> &mut Self {
        self.commit_context();
        self.operations.push(WriteOperation::InsertOne { document });
        self
    }

    pub fn build(&mut self) -> &mut Self {
        self.commit_context();
        self
    }
 
        /*
        filter_map 的工作方式是：
        对每个元素应用转换函数
        如果函数返回 Some(x)，则保留值 x
        如果函数返回 None，则跳过该元素
        所以：
        空的过滤条件（filter.is_empty()）会返回 None
        没有更新操作的更新（updates.is_empty()）会返回 None
        这些 None 值会被 filter_map 自动过滤掉
        最终的 write_models 只包含有效的操作
        这是一个安全的实现，可以防止意外的全表更新或删除操作。
     */
    pub fn execute(&mut self) -> BulkWrite<SummaryBulkWriteResult> {
        self.build();
        // 原有的 execute 实现保持不变
        let write_models: Vec<WriteModel> = self.operations.iter()
            .filter_map(|operation| match operation {
                WriteOperation::UpdateOne { filter, updates, upsert } => {
                    // 如果 filter为{} 则不进行更新，因为这是一个破坏性极大的操作，会把整个表的数据都更新，这种操作要禁止
                    if !filter.is_empty() && !updates.is_empty() {
                        let mut update_doc = Document::new();
                        for update in updates {
                            let doc = update.to_document();
                            for (k, v) in doc.iter() {
                                update_doc.insert(k, v.clone());
                            }
                        }
                        
                        Some(UpdateOneModel::builder()
                            .namespace(self.collection.namespace())
                            .filter(filter.clone())
                            .update(UpdateModifications::Document(update_doc))
                            .upsert(Some(*upsert))
                            .build()
                            .into())
                    } else {
                        None
                    }
                },
                WriteOperation::DeleteOne { filter } => {
                    if !filter.is_empty() {
                        Some(DeleteOneModel::builder()
                            .namespace(self.collection.namespace())
                            .filter(filter.clone())
                            .build()
                            .into())
                    } else {
                        None
                    }
                },
                WriteOperation::DeleteMany { filter } => {
                    if !filter.is_empty() {
                        Some(DeleteManyModel::builder()
                            .namespace(self.collection.namespace())
                            .filter(filter.clone())
                            .build()
                            .into())
            } else {
                None
                    }
                },
                WriteOperation::InsertOne { document } => {
                    Some(InsertOneModel::builder()
                        .namespace(self.collection.namespace())
                        .document(to_document(document).unwrap())
                        .build()
                        .into())
                },
            })
            .collect();

        self.collection.client().bulk_write(write_models)
    }
  
}

impl BatchUpdateContext{
    fn add_update_type(&mut self, new_update: UpdateType) {
        // 尝试合并相同类型的操作
        let mut merged = false;
        for existing_update in &mut self.updates {
            if let Some(merged_update) = existing_update.merge(&new_update) {
                *existing_update = merged_update;
                merged = true;
                break;
            }
        }
        
        if !merged {
            self.updates.push(new_update);
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::{Client, bson::doc};
    use crate::models::order::Order;

    async fn get_test_collection() -> Collection<Order> {
        let client = Client::with_uri_str("mongodb://localhost:27017").await.unwrap();
        client.database("test").collection("test_collection")
    }

    #[tokio::test]
    async fn test_different_update_types_same_filter() {
        let collection = get_test_collection().await;
        let mut builder: BatchUpdateBuilder<Order> = BatchUpdateBuilder::new(collection);
        let filter = doc! { "id": 1 };

        builder.add_update(filter.clone())
                        .set(doc! { "name": "test1" })
                        .inc(doc! { "count": 1 });
                        //.build();

        assert_eq!(builder.operations.len(), 1);
        if let WriteOperation::UpdateOne { updates, .. } = &builder.operations[0] {
            assert_eq!(updates.len(), 2);
            match (&updates[0], &updates[1]) {
                (UpdateType::Set(set_doc), UpdateType::Inc(inc_doc)) => {
                    assert_eq!(set_doc.get("name").unwrap().as_str().unwrap(), "test1");
                    assert_eq!(inc_doc.get("count").unwrap().as_i32().unwrap(), 1);
                }
                _ => panic!("Unexpected update types")
            }
        } else {
            panic!("Expected UpdateOne operation");
        }
    }

    #[tokio::test]
    async fn test_merge_same_update_type_and_filter() {
        let collection = get_test_collection().await;
        let mut builder: BatchUpdateBuilder<Order> = BatchUpdateBuilder::new(collection);
        let filter = doc! { "id": 1 };

        builder.add_update(filter.clone())
            .set(doc! { "name": "test1" })
            .set(doc! { "age": 20 });
            //.build();

        assert_eq!(builder.operations.len(), 1);
        if let WriteOperation::UpdateOne { updates, .. } = &builder.operations[0] {
            assert_eq!(updates.len(), 1);
            if let UpdateType::Set(doc) = &updates[0] {
                assert_eq!(doc.get("name").unwrap().as_str().unwrap(), "test1");
                assert_eq!(doc.get("age").unwrap().as_i32().unwrap(), 20);
            } else {
                panic!("Expected Set update type");
            }
        } else {
            panic!("Expected UpdateOne operation");
        }
    }

    #[tokio::test]
    async fn test_different_filters_no_merge() {
        let collection = get_test_collection().await;
        let mut builder: BatchUpdateBuilder<Order> = BatchUpdateBuilder::new(collection);
        
        builder.add_update(doc! { "id": 1 })
            .set(doc! { "name": "test1" })
            //.build()
            .add_update(doc! { "id": 2 })
            .set(doc! { "name": "test2" });
            //.build();

        assert_eq!(builder.operations.len(), 2);
    }

    #[tokio::test]
    async fn test_mixed_operations() {
        let collection = get_test_collection().await;
        let mut builder: BatchUpdateBuilder<Order> = BatchUpdateBuilder::new(collection);
        
        builder.add_update(doc! { "id": 1 })
            .set(doc! { "name": "test1" })
            //.build()
            //.add_update(doc! { "id": 2 })
            .delete(doc! { "id": 2 })
            .add_update(doc! { "id": 3 })
            .set(doc! { "name": "test3" }); 
            //.build();

        assert_eq!(builder.operations.len(), 3);
        
        match &builder.operations[0] {
            WriteOperation::UpdateOne { .. } => (),
            _ => panic!("Expected UpdateOne operation")
        }
        
        match &builder.operations[1] {
            WriteOperation::DeleteOne { .. } => (),
            _ => panic!("Expected DeleteOne operation")
        }
        
        match &builder.operations[2] {
            WriteOperation::UpdateOne { .. } => (),
            _ => panic!("Expected UpdateOne operation")
        }
    }
}
