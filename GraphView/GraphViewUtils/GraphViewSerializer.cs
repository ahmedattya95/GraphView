using System;
using System.Collections.Generic;
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

        internal static void Serialize(GraphViewCommand command, GraphViewExecutionOperator op)
        {
            bool onlyCompile = command.OnlyCompile;
            command.OnlyCompile = false;
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

            command.OnlyCompile = onlyCompile;
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

    }

    internal static class SerializationData
    {
        public static Dictionary<string, IAggregateFunction> SideEffectStates { get; private set; } = new Dictionary<string, IAggregateFunction>();

        public static List<Container> Containers { get; private set; } = new List<Container>();
        public static int index = 0;

        public static GraphViewCommand Command { get; private set; }

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
    }
}
