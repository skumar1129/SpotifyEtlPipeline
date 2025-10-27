"""
Helper script to run a Great Expectations checkpoint.
This is called by a PythonOperator or BashOperator in Airflow.
"""

import sys
import argparse
import great_expectations as ge

def run_validation(ge_project_root, checkpoint_name):
    """
    Loads a GE project and runs a specific checkpoint.
    
    Raises SystemExit if validation fails.
    """
    print(f"Loading Great Expectations project from: {ge_project_root}")
    try:
        context = ge.get_context(context_root_dir=ge_project_root)
    except Exception as e:
        print(f"Error: Could not load Great Expectations context. {e}")
        sys.exit(1)
        
    print(f"Running checkpoint: {checkpoint_name}")
    try:
        checkpoint_result = context.run_checkpoint(checkpoint_name=checkpoint_name)
    except Exception as e:
        print(f"Error: Failed to run checkpoint '{checkpoint_name}'. {e}")
        sys.exit(1)

    if not checkpoint_result["success"]:
        print("Validation FAILED!")
        print(checkpoint_result)
        # Fail the script, which fails the Airflow task
        sys.exit(1)
    else:
        print("Validation SUCCEEDED!")
        print(checkpoint_result)
        sys.exit(0)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--ge_project_root",
        required=True,
        help="The root directory of the Great Expectations project.",
    )
    parser.add_argument(
        "--checkpoint_name",
        required=True,
        help="The name of the checkpoint to run.",
    )
    
    args = parser.parse_args()
    
    run_validation(
        ge_project_root=args.ge_project_root,
        checkpoint_name=args.checkpoint_name
    )
