import grpc
from proto import llm_pb2, llm_pb2_grpc
from auth import get_current_user  # implement this

def get_auth_metadata():
    user = get_current_user()
    if not user:
        raise Exception("User not authenticated")
    # gRPC metadata: list of key-value tuples
    return [('authorization', f'Bearer {user["token"]}')], user['role']

def main():
    channel = grpc.insecure_channel('localhost:50051')
    stub = llm_pb2_grpc.LLMServiceStub(channel)

    metadata, role = get_auth_metadata()
    text = input("Enter your query: ")

    request = llm_pb2.LLMRequest(text=text, role=role)
    response = stub.GetPrediction(request, metadata=metadata)

    print("LLM Response:", response.reply)

if __name__ == "__main__":
    main()
