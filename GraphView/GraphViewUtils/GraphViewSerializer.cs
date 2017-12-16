using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Soap;
using System.Text;
using System.Threading.Tasks;
using GraphView.GraphViewDBPortal;

namespace GraphView
{
    public static class GraphViewSerializer
    {
        public const string CommandFile = "command.xml";
        public const string OperatorsFile = "plan.xml";
        public const string SideEffectFile = "sideEffect.xml";
        public const string ContainerFile = "container.xml";
        public const string PartitionPlanFile = "partitionPlan.xml";

        internal static void Serialize(GraphViewCommand command, GraphViewExecutionOperator op)
        {
            SoapFormatter serilizer = new SoapFormatter();
            Stream stream = File.Open(CommandFile, FileMode.Create);
            serilizer.Serialize(stream, command);
            stream.Close();

            stream = File.Open(ContainerFile, FileMode.Create);
            DataContractSerializer containerSer = new DataContractSerializer(typeof(List<Container>));
            containerSer.WriteObject(stream, SerializationData.Containers);
            stream.Close();

            stream = File.Open(SideEffectFile, FileMode.Create);
            DataContractSerializer sideEffectSer = new DataContractSerializer(typeof(Dictionary<string, IAggregateFunction>),
                new List<Type>{typeof(CollectionFunction), typeof(GroupFunction), typeof(SubgraphFunction), typeof(TreeFunction) });
            sideEffectSer.WriteObject(stream, SerializationData.SideEffectStates);
            stream.Close();

            stream = File.Open(OperatorsFile, FileMode.Create);
            DataContractSerializer ser = new DataContractSerializer(typeof(GraphViewExecutionOperator));
            ser.WriteObject(stream, op);
            stream.Close();
        }

        internal static GraphViewExecutionOperator Deserialize(out GraphViewCommand command)
        {
            SoapFormatter deserilizer = new SoapFormatter();
            Stream stream = File.Open(CommandFile, FileMode.Open);
            command = (GraphViewCommand)deserilizer.Deserialize(stream);
            SerializationData.SetCommand(command);
            stream.Close();

            stream = File.Open(ContainerFile, FileMode.Open);
            DataContractSerializer containerDeser = new DataContractSerializer(typeof(List<Container>));
            SerializationData.SetContainers((List<Container>)containerDeser.ReadObject(stream));
            stream.Close();

            stream = File.Open(SideEffectFile, FileMode.Open);
            DataContractSerializer sideEffectDeser = new DataContractSerializer(typeof(Dictionary<string, IAggregateFunction>),
                new List<Type> { typeof(CollectionFunction), typeof(GroupFunction), typeof(SubgraphFunction), typeof(TreeFunction) });
            SerializationData.SetSideEffectStates((Dictionary<string, IAggregateFunction>)sideEffectDeser.ReadObject(stream));
            stream.Close();

            stream = File.Open(OperatorsFile, FileMode.Open);
            DataContractSerializer deser = new DataContractSerializer(typeof(GraphViewExecutionOperator));
            GraphViewExecutionOperator op = (GraphViewExecutionOperator)deser.ReadObject(stream);
            stream.Close();

            op.ResetState();
            return op;
        }

        public static void SerializePatitionPlan(List<PartitionPlan> PartitionPlans)
        {
            Stream stream = File.Open(PartitionPlanFile, FileMode.Create);
            DataContractSerializer ser = new DataContractSerializer(typeof(List<PartitionPlan>));
            ser.WriteObject(stream, PartitionPlans);
            stream.Close();
        }

        internal static void DeserializePatitionPlan()
        {
            Stream stream = File.Open(PartitionPlanFile, FileMode.Open);
            DataContractSerializer deser = new DataContractSerializer(typeof(List<PartitionPlan>));
            int index = int.Parse(Environment.GetEnvironmentVariable("AZ_BATCH_TASK_ID")??"0");
            SerializationData.SetPartitionPlan(((List<PartitionPlan>)deser.ReadObject(stream))[index]);
            stream.Close();
        }
    }

    public enum PartitionCompareType
    {
        Equal,
        In,
        Between
    }

    public enum PartitionBetweenType
    {
        IncludeBoth, // a <= x <= b
        IncludeLeft, // a <= x < b
        IncludeRight, // a < x <=b
        Greater, // a < x
        GreaterOrEqual, // a <= x
        Less, // x < b
        LessOrEqual // x <= b
    }

    public enum PartitionMethod
    {
        CompareFirstChar
    }

    [DataContract]
    public class PartitionPlan
    {
        [DataMember]
        private string partitionKey;

        [DataMember]
        private PartitionCompareType compareType;
        [DataMember]
        private string equalValue;
        [DataMember]
        private List<string> inValues;
        [DataMember]
        private Tuple<string, string, PartitionBetweenType> betweenValues;

        [DataMember]
        private PartitionMethod partitionMethod;

        public PartitionPlan(string partitionKey, PartitionMethod partitionMethod, string equalValue)
        {
            this.partitionKey = partitionKey;
            this.partitionMethod = partitionMethod;
            this.equalValue = equalValue;
            this.compareType = PartitionCompareType.Equal;
        }

        public PartitionPlan(string partitionKey, PartitionMethod partitionMethod, List<string> inValues)
        {
            this.partitionKey = partitionKey;
            this.partitionMethod = partitionMethod;
            this.inValues = inValues;
            this.compareType = PartitionCompareType.In;
        }

        public PartitionPlan(string partitionKey, PartitionMethod partitionMethod, Tuple<string, string, PartitionBetweenType> betweenValues)
        {
            this.partitionKey = partitionKey;
            this.partitionMethod = partitionMethod;
            this.betweenValues = betweenValues;
            this.compareType = PartitionCompareType.Between;
        }

        internal string AppendToWhereClause(string tableAlias, string whereClause)
        {
            if (this.partitionMethod == PartitionMethod.CompareFirstChar)
            {
                if (this.compareType == PartitionCompareType.Between)
                {
                    string leftSymbol = "<";
                    string rightSymbol = "<";
                    switch (this.betweenValues.Item3)
                    {
                        case PartitionBetweenType.IncludeBoth:
                            leftSymbol += "=";
                            rightSymbol += "=";
                            break;
                        case PartitionBetweenType.IncludeLeft:
                            leftSymbol += "=";
                            break;
                        case PartitionBetweenType.IncludeRight:
                            rightSymbol += "=";
                            break;
                        case PartitionBetweenType.GreaterOrEqual:
                            leftSymbol += "=";
                            break;
                        case PartitionBetweenType.LessOrEqual:
                            rightSymbol += "=";
                            break;
                    }

                    switch (this.betweenValues.Item3)
                    {
                        case PartitionBetweenType.IncludeBoth:
                        case PartitionBetweenType.IncludeLeft:
                        case PartitionBetweenType.IncludeRight:
                            return $"({whereClause}) AND " +
                                   $"\"{this.betweenValues.Item1}\" {leftSymbol} LOWER(LEFT({tableAlias}.{this.partitionKey}, 1)) " +
                                   $"AND LOWER(LEFT({tableAlias}.{this.partitionKey}, 1)) {rightSymbol} \"{this.betweenValues.Item2}\" ";
                        case PartitionBetweenType.Greater:
                        case PartitionBetweenType.GreaterOrEqual:
                            return $"({whereClause}) AND " +
                                   $"\"{this.betweenValues.Item1}\" {leftSymbol} LOWER(LEFT({tableAlias}.{this.partitionKey}, 1)) ";
                        case PartitionBetweenType.Less:
                        case PartitionBetweenType.LessOrEqual:
                            return $"({whereClause}) AND " +
                                   $"LOWER(LEFT({tableAlias}.{this.partitionKey}, 1)) {rightSymbol} \"{this.betweenValues.Item2}\" ";
                        default:
                            throw new NotImplementedException();
                    }
                }
                else
                {
                    throw new NotImplementedException();
                }
            }
            else
            {
                throw new NotImplementedException();
            }
        }
    }

    internal static class SerializationData
    {
        public static Dictionary<string, IAggregateFunction> SideEffectStates { get; private set; } = new Dictionary<string, IAggregateFunction>();

        public static List<Container> Containers { get; private set; } = new List<Container>();
        public static int index = 0;

        public static GraphViewCommand Command { get; private set; }

        public static PartitionPlan partitionPlan { get; private set; }

        public static void SetSideEffectStates(Dictionary<string, IAggregateFunction> sideEffectStates)
        {
            SerializationData.SideEffectStates = sideEffectStates;
        }

        public static void SetContainers(List<Container> containers)
        {
            SerializationData.Containers = containers;
        }

        public static void SetCommand(GraphViewCommand command)
        {
            SerializationData.Command = command;
        }

        public static void SetPartitionPlan(PartitionPlan partitionPlan)
        {
            SerializationData.partitionPlan = partitionPlan;
        }
    }
}
