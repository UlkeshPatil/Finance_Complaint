from finance_complaint.pipeline.batch_prediction import BatchPrediction
from finance_complaint.entity.config_entity import BatchPredictionConfig

if __name__=="__main__":
    batch_config = BatchPredictionConfig()
    batch_pred = BatchPrediction(batch_config=batch_config)
    batch_pred.start_prediction()